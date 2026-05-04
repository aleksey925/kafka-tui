// Package vault implements a small KV v2 client used to resolve
// `${vault:path[#key]}` placeholders in kafka-tui configuration.
//
// The client speaks raw HTTP against Vault's `/v1/<mount>/data/<key>` API to
// avoid pulling in the heavy `github.com/hashicorp/vault/api` SDK. Each
// distinct path is fetched at most once per Client instance — multiple
// placeholders sharing a path (different keys) cause a single network call.
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
)

const (
	// DefaultTimeout is the per-request HTTP timeout when none is set.
	DefaultTimeout = 10 * time.Second
	// EnvVaultToken is the environment variable consulted as the second
	// step of token resolution.
	EnvVaultToken = "VAULT_TOKEN"
	// TokenFileName is the name of the file in the user's home directory
	// consulted as the final step of token resolution.
	TokenFileName = ".vault-token"
)

// Options configures NewClient.
type Options struct {
	// Address is the Vault server URL (e.g. https://vault.example.com:8200).
	Address string
	// Token is the explicit Vault token. When non-empty, it has the highest
	// priority and skips the env / token-file fallbacks.
	Token string
	// Timeout caps each HTTP request. Zero defaults to DefaultTimeout.
	Timeout time.Duration

	// HomeDir overrides $HOME for ~/.vault-token resolution (used by tests).
	HomeDir string
	// HTTPClient overrides the default HTTP client (used by tests).
	HTTPClient *http.Client
	// EnvLookup overrides os.LookupEnv for $VAULT_TOKEN resolution (used by tests).
	EnvLookup func(string) (string, bool)
	// FileReader overrides os.ReadFile for token-file resolution (used by tests).
	FileReader func(string) ([]byte, error)
}

// Client is a minimal Vault KV v2 reader.
type Client struct {
	addr  string
	token string
	http  *http.Client

	mu    sync.Mutex
	cache map[string]map[string]any
}

// NewClient constructs a Client. It resolves the token in this order:
// explicit Token → $VAULT_TOKEN → ~/.vault-token. If none yields a value,
// it returns an error.
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
// secret payload is returned as a JSON-encoded string.
//
// Lookup uses a cache keyed by path so that multiple placeholders pointing
// at the same secret only trigger one HTTP request.
func (c *Client) Lookup(path, key string) (string, error) {
	return c.LookupContext(context.Background(), path, key)
}

// LookupContext is the context-aware variant of Lookup.
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

// fetch returns the cached secret data for path, fetching from Vault if not
// yet seen. The mutex guards the cache map; the HTTP request runs unlocked
// so a slow Vault does not stall unrelated lookups.
func (c *Client) fetch(ctx context.Context, path string) (map[string]any, error) {
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

// resolveToken applies the documented fallback chain.
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

// toAPIPath converts a logical KV v2 path (e.g. "secret/foo/bar") to its API
// path form ("secret/data/foo/bar") by inserting "data" after the first
// segment. Leading and trailing slashes are trimmed.
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

// stringify renders a single field of the secret as a string. Strings pass
// through verbatim; everything else is JSON-encoded so callers (e.g. config
// placeholder resolution) always get a usable scalar.
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
