package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient__emptyAddress__error(t *testing.T) {
	// act
	_, err := NewClient(Options{Address: "", Token: "t"})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "address is empty")
}

func TestNewClient__explicitTokenWins(t *testing.T) {
	// arrange
	calls := atomic.Int32{}
	envLookup := func(string) (string, bool) {
		calls.Add(1)
		return "from-env", true
	}

	// act
	c, err := NewClient(Options{
		Address:   "http://vault.example.com",
		Token:     "explicit",
		EnvLookup: envLookup,
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, "explicit", c.token)
	assert.Equal(t, int32(0), calls.Load(), "env should not be consulted when token is explicit")
}

func TestNewClient__envFallback(t *testing.T) {
	// arrange
	envLookup := func(name string) (string, bool) {
		if name == EnvVaultToken {
			return "env-token", true
		}
		return "", false
	}

	// act
	c, err := NewClient(Options{
		Address:   "http://v",
		EnvLookup: envLookup,
		HomeDir:   t.TempDir(), // empty dir keeps file-fallback inert
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, "env-token", c.token)
}

func TestNewClient__tokenFileFallback(t *testing.T) {
	// arrange
	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(home, TokenFileName), []byte("file-token\n"), 0o600))

	// act
	c, err := NewClient(Options{
		Address:   "http://v",
		HomeDir:   home,
		EnvLookup: func(string) (string, bool) { return "", false },
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, "file-token", c.token)
}

func TestNewClient__noToken__error(t *testing.T) {
	// arrange — empty home dir + empty env
	home := t.TempDir()

	// act
	_, err := NewClient(Options{
		Address:   "http://v",
		HomeDir:   home,
		EnvLookup: func(string) (string, bool) { return "", false },
	})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token not configured")
}

func TestNewClient__envEmptyValue__fallsThroughToFile(t *testing.T) {
	// arrange — VAULT_TOKEN is set but blank; should not satisfy
	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(home, TokenFileName), []byte("file"), 0o600))

	// act
	c, err := NewClient(Options{
		Address:   "http://v",
		HomeDir:   home,
		EnvLookup: func(string) (string, bool) { return "   ", true },
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, "file", c.token)
}

func TestNewClient__tokenFileReadError__propagates(t *testing.T) {
	// arrange
	boom := errors.New("permission denied")

	// act
	_, err := NewClient(Options{
		Address:    "http://v",
		HomeDir:    "/some/home",
		EnvLookup:  func(string) (string, bool) { return "", false },
		FileReader: func(string) ([]byte, error) { return nil, boom },
	})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
}

func TestLookup__keyValue(t *testing.T) {
	// arrange
	srv := newMockVault(t, map[string]map[string]any{
		"secret/data/db": {"password": "p4ss", "user": "alice"},
	})
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	got, err := c.Lookup("secret/db", "password")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "p4ss", got)
}

func TestLookup__missingKey__error(t *testing.T) {
	// arrange
	srv := newMockVault(t, map[string]map[string]any{
		"secret/data/db": {"user": "alice"},
	})
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	_, err := c.Lookup("secret/db", "password")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), `key "password"`)
	assert.Contains(t, err.Error(), `"secret/db"`)
}

func TestLookup__wholeSecret_returnsJSON(t *testing.T) {
	// arrange
	srv := newMockVault(t, map[string]map[string]any{
		"secret/data/db": {"password": "p4ss"},
	})
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	got, err := c.Lookup("secret/db", "")

	// assert
	require.NoError(t, err)
	assert.JSONEq(t, `{"password":"p4ss"}`, got)
}

func TestLookup__nonStringValue_isJSONEncoded(t *testing.T) {
	// arrange
	srv := newMockVault(t, map[string]map[string]any{
		"secret/data/numbers": {"port": 5432.0, "tls": true},
	})
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	port, errPort := c.Lookup("secret/numbers", "port")
	tls, errTLS := c.Lookup("secret/numbers", "tls")

	// assert
	require.NoError(t, errPort)
	require.NoError(t, errTLS)
	assert.Equal(t, "5432", port)
	assert.Equal(t, "true", tls)
}

func TestLookup__notFound__error(t *testing.T) {
	// arrange
	srv := newMockVault(t, nil)
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	_, err := c.Lookup("secret/missing", "key")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret not found")
}

func TestLookup__unauthorized__error(t *testing.T) {
	// arrange
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	_, err := c.Lookup("secret/db", "k")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid token")
}

func TestLookup__forbidden__error(t *testing.T) {
	// arrange
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	_, err := c.Lookup("secret/db", "k")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
}

func TestLookup__serverError__includesStatus(t *testing.T) {
	// arrange
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "kaboom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	_, err := c.Lookup("secret/db", "k")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
	assert.Contains(t, err.Error(), "kaboom")
}

func TestLookup__invalidJSON__error(t *testing.T) {
	// arrange
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "not-json")
	}))
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	_, err := c.Lookup("secret/db", "k")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse response")
}

func TestLookup__sendsTokenAndExpectedURL(t *testing.T) {
	// arrange
	var seenURL, seenToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenURL = r.URL.Path
		seenToken = r.Header.Get("X-Vault-Token")
		_, _ = io.WriteString(w, `{"data":{"data":{"k":"v"}}}`)
	}))
	defer srv.Close()
	c := mustClient(t, srv.URL, "secret-token")

	// act
	_, err := c.Lookup("kv/myapp/db", "k")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "/v1/kv/data/myapp/db", seenURL)
	assert.Equal(t, "secret-token", seenToken)
}

func TestLookup__cachesByPath(t *testing.T) {
	// arrange
	hits := atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `{"data":{"data":{"a":"1","b":"2"}}}`)
	}))
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act — three lookups against the same path
	a, errA := c.Lookup("secret/db", "a")
	b, errB := c.Lookup("secret/db", "b")
	whole, errWhole := c.Lookup("secret/db", "")

	// assert — single HTTP call, all results derived from it
	require.NoError(t, errA)
	require.NoError(t, errB)
	require.NoError(t, errWhole)
	assert.Equal(t, "1", a)
	assert.Equal(t, "2", b)
	assert.JSONEq(t, `{"a":"1","b":"2"}`, whole)
	assert.Equal(t, int32(1), hits.Load(), "Vault should be hit at most once per path")
}

func TestLookup__differentPaths_separateCalls(t *testing.T) {
	// arrange
	hits := atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/v1/secret/data/a":
			_, _ = io.WriteString(w, `{"data":{"data":{"k":"av"}}}`)
		case "/v1/secret/data/b":
			_, _ = io.WriteString(w, `{"data":{"data":{"k":"bv"}}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := mustClient(t, srv.URL, "tok")

	// act
	av, errA := c.Lookup("secret/a", "k")
	bv, errB := c.Lookup("secret/b", "k")

	// assert
	require.NoError(t, errA)
	require.NoError(t, errB)
	assert.Equal(t, "av", av)
	assert.Equal(t, "bv", bv)
	assert.Equal(t, int32(2), hits.Load())
}

func TestLookup__emptyPath__error(t *testing.T) {
	// arrange
	c := mustClient(t, "http://v", "tok")

	// act
	_, err := c.Lookup("", "key")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty path")
}

func TestLookupContext__honorsCancellation(t *testing.T) {
	// arrange — server blocks until the test releases it
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = io.WriteString(w, `{"data":{"data":{"k":"v"}}}`)
	}))
	defer srv.Close()
	defer close(release)

	c := mustClient(t, srv.URL, "tok")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel: request should fail without waiting

	// act
	_, err := c.LookupContext(ctx, "secret/db", "k")

	// assert
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLookup__timesOutQuickly(t *testing.T) {
	// arrange — server hangs longer than the configured timeout
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-hold
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	defer close(hold)

	c, err := NewClient(Options{
		Address: srv.URL,
		Token:   "tok",
		Timeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// act
	start := time.Now()
	_, err = c.Lookup("secret/db", "k")
	elapsed := time.Since(start)

	// assert
	require.Error(t, err)
	assert.Less(t, elapsed, time.Second, "timeout should fire well before 1s, took %s", elapsed)
}

func TestToAPIPath(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"secret/foo/bar", "secret/data/foo/bar"},
		{"kv/app/db", "kv/data/app/db"},
		{"/secret/foo/", "secret/data/foo"},
		{"secret/foo", "secret/data/foo"},
		{"secret", "secret"},
		{"", ""},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			assert.Equal(t, c.want, toAPIPath(c.in))
		})
	}
}

// --- helpers ---

func mustClient(t *testing.T, addr, token string) *Client {
	t.Helper()
	c, err := NewClient(Options{Address: addr, Token: token})
	require.NoError(t, err)
	return c
}

// newMockVault returns an httptest server that serves canned KV v2 responses
// for the given paths (keyed by their API form, e.g. "secret/data/db").
// Unknown paths return 404.
func newMockVault(t *testing.T, secrets map[string]map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "/v1/"
		path := r.URL.Path
		if len(path) <= len(prefix) || path[:len(prefix)] != prefix {
			http.NotFound(w, r)
			return
		}
		key := path[len(prefix):]
		data, ok := secrets[key]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"data":{"data":%s}}`, mustJSON(t, data))
	}))
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}
