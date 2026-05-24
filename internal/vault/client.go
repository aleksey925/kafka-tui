// Package vault implements a minimal KV v2 client for resolving
// `${vault:path[#key]}` placeholders. It speaks raw HTTP against
// `/v1/<mount>/data/<key>` to avoid the heavy hashicorp/vault/api SDK, and
// caches each distinct path so multiple placeholders against the same secret
// only trigger one network call.
package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	DefaultTimeout = 10 * time.Second
	EnvVaultToken  = "VAULT_TOKEN"
	TokenFileName  = ".vault-token"
)

// Options configures NewClient. Token, when non-empty, skips the env /
// token-file fallbacks. Zero Timeout falls back to DefaultTimeout.
type Options struct {
	Address string
	Token   string
	Timeout time.Duration

	HomeDir    string
	HTTPClient *http.Client
	EnvLookup  func(string) (string, bool)
	FileReader func(string) ([]byte, error)
}

type Client struct {
	addr  string
	token string
	http  *http.Client

	mu       sync.Mutex
	cache    map[string]map[string]any
	inFlight singleflight.Group
}

// NewClient constructs a Client. Token resolution order:
// explicit Token → $VAULT_TOKEN → ~/.vault-token.
func NewClient(opts Options) (*Client, error) {
	addr := strings.TrimRight(strings.TrimSpace(opts.Address), "/")
	if addr == "" {
		return nil, errors.New("vault: address is empty")
	}

	token, err := resolveToken(opts)
	if err != nil {
		return nil, err
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		timeout := opts.Timeout
		if timeout == 0 {
			timeout = DefaultTimeout
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	return &Client{
		addr:  addr,
		token: token,
		http:  httpClient,
		cache: map[string]map[string]any{},
	}, nil
}

// Lookup implements config.VaultResolver. When key is empty, the entire
// secret payload is returned as a JSON-encoded string. Results are cached
// per path.
func (c *Client) Lookup(path, key string) (string, error) {
	return c.LookupContext(context.Background(), path, key)
}

func (c *Client) LookupContext(ctx context.Context, path, key string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("vault: empty path")
	}

	data, err := c.fetch(ctx, path)
	if err != nil {
		return "", err
	}

	if key == "" {
		encoded, err := json.Marshal(data)
		if err != nil {
			return "", fmt.Errorf("vault: marshal secret at %q: %w", path, err)
		}
		return string(encoded), nil
	}

	v, ok := data[key]
	if !ok {
		return "", fmt.Errorf("vault: key %q not found at %q", key, path)
	}
	return stringify(v)
}

// fetch returns the cached secret for path. The mutex guards only the cache
// map — HTTP runs unlocked so a slow Vault does not stall unrelated lookups.
// singleflight collapses concurrent misses for the same path into one
// request: at startup multiple clusters often resolve the same secret, and
// N kgo.Dial races used to fire N HTTP calls instead of one.
func (c *Client) fetch(ctx context.Context, path string) (map[string]any, error) {
	c.mu.Lock()
	if data, ok := c.cache[path]; ok {
		c.mu.Unlock()
		return data, nil
	}
	c.mu.Unlock()

	v, err, _ := c.inFlight.Do(path, func() (any, error) {
		// double-check under the same flight: another goroutine may have
		// finished and cached between our miss above and singleflight
		// admitting us.
		c.mu.Lock()
		if data, ok := c.cache[path]; ok {
			c.mu.Unlock()
			return data, nil
		}
		c.mu.Unlock()

		data, err := c.doRequest(ctx, path)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.cache[path] = data
		c.mu.Unlock()
		return data, nil
	})
	if err != nil {
		return nil, fmt.Errorf("vault: %w", err)
	}
	return v.(map[string]any), nil
}

func (c *Client) doRequest(ctx context.Context, path string) (map[string]any, error) {
	url := c.addr + "/v1/" + toAPIPath(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("vault: build request for %q: %w", path, err)
	}
	req.Header.Set("X-Vault-Token", c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault: GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("vault: read response for %q: %w", path, err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, fmt.Errorf("vault: secret not found at %q", path)
	case http.StatusUnauthorized:
		return nil, fmt.Errorf("vault: invalid token while reading %q", path)
	case http.StatusForbidden:
		return nil, fmt.Errorf("vault: permission denied for %q", path)
	default:
		return nil, fmt.Errorf(
			"vault: GET %q returned status %d: %s",
			path, resp.StatusCode, strings.TrimSpace(string(body)),
		)
	}

	var parsed struct {
		Data struct {
			Data map[string]any `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("vault: parse response for %q: %w", path, err)
	}
	if parsed.Data.Data == nil {
		parsed.Data.Data = map[string]any{}
	}
	return parsed.Data.Data, nil
}

// resolveToken applies the fallback chain: opts.Token → $VAULT_TOKEN →
// ~/.vault-token.
func resolveToken(opts Options) (string, error) {
	if t := strings.TrimSpace(opts.Token); t != "" {
		return t, nil
	}

	envLookup := opts.EnvLookup
	if envLookup == nil {
		envLookup = os.LookupEnv
	}
	if v, ok := envLookup(EnvVaultToken); ok {
		if t := strings.TrimSpace(v); t != "" {
			return t, nil
		}
	}

	homeDir := opts.HomeDir
	if homeDir == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("vault: cannot resolve home dir for token lookup: %w", err)
		}
		homeDir = h
	}

	read := opts.FileReader
	if read == nil {
		read = os.ReadFile
	}
	tokenPath := filepath.Join(homeDir, TokenFileName)
	data, err := read(tokenPath)
	switch {
	case err == nil:
		if t := strings.TrimSpace(string(data)); t != "" {
			return t, nil
		}
	case errors.Is(err, os.ErrNotExist):
		// fall through to the final error below
	default:
		return "", fmt.Errorf("vault: read token file %s: %w", tokenPath, err)
	}

	return "", errors.New(
		"vault: token not configured (set vault.token, $VAULT_TOKEN, or write ~/.vault-token)",
	)
}

// toAPIPath converts a logical KV v2 path ("secret/foo/bar") to its API form
// ("secret/data/foo/bar") by inserting "data" after the first segment.
func toAPIPath(logical string) string {
	trimmed := strings.Trim(logical, "/")
	if trimmed == "" {
		return ""
	}
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 2 {
		return trimmed
	}
	return parts[0] + "/data/" + parts[1]
}

// stringify renders a secret field as a string. Strings pass through
// verbatim; everything else is JSON-encoded so callers always get a scalar.
func stringify(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case nil:
		return "", nil
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return "", fmt.Errorf("vault: encode value: %w", err)
		}
		return string(b), nil
	}
}
