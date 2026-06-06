package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
)

const (
	placeholderEnv   = "env"
	placeholderFile  = "file"
	placeholderVault = "vault"
)

// IsLiteralCredential reports whether value is a non-empty literal
// rather than a placeholder reference. Shared detector for every
// credential-exposure check — see CLAUDE.md § Credentials: storage
// and exposure warnings. Generic so [Secret] and raw string CLI-flag
// values share one implementation.
func IsLiteralCredential[T ~string](value T) bool {
	v := strings.TrimSpace(string(value))
	return v != "" && !strings.HasPrefix(v, "${")
}

// VaultResolver fetches a Vault secret for ${vault:path[#key]} placeholders.
// When key is empty, the entire secret payload is returned as a string
// (typically JSON — the exact representation is up to the implementation).
type VaultResolver interface {
	Lookup(path, key string) (string, error)
}

type EnvLookup func(name string) (string, bool)

type FileReader func(path string) ([]byte, error)

// Resolvers configures placeholder resolution. A nil resolver leaves the
// corresponding placeholder kind intact in the output, which lets callers run
// resolution in phases (env+file first, vault second) per the specification.
type Resolvers struct {
	Env   EnvLookup
	File  FileReader
	Vault VaultResolver
}

// EnvFileResolvers returns the first phase of two-phase resolution.
func EnvFileResolvers() Resolvers {
	return Resolvers{Env: os.LookupEnv, File: os.ReadFile}
}

// VaultOnlyResolvers returns the second phase of two-phase resolution.
func VaultOnlyResolvers(v VaultResolver) Resolvers {
	return Resolvers{Vault: v}
}

// ResolveString leaves placeholder kinds whose resolver is nil intact so
// callers can run multiple phases.
func (r Resolvers) ResolveString(s string) (string, error) {
	if !strings.Contains(s, "${") {
		return s, nil
	}
	parts, err := scanPlaceholders(s)
	if err != nil {
		return "", err
	}
	if len(parts) == 0 {
		return s, nil
	}
	var b strings.Builder
	cursor := 0
	for _, p := range parts {
		b.WriteString(s[cursor:p.start])
		replaced, ok, resErr := r.resolveOne(p)
		if resErr != nil {
			return "", resErr
		}
		if ok {
			b.WriteString(replaced)
		} else {
			b.WriteString(p.raw)
		}
		cursor = p.end
	}
	b.WriteString(s[cursor:])
	return b.String(), nil
}

// ResolveStruct walks v reflectively and resolves placeholders in every
// settable string field.
func (r Resolvers) ResolveStruct(v any) error {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil
	}
	return r.resolveValue(rv)
}

func (r Resolvers) resolveValue(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return r.resolveValue(v.Elem())
	case reflect.Struct:
		t := v.Type()
		for i := range v.NumField() {
			// a `placeholder:"-"` field is resolved on the per-cluster
			// path, not here — see CLAUDE.md § Cluster loading.
			if t.Field(i).Tag.Get("placeholder") == "-" {
				continue
			}
			f := v.Field(i)
			if !f.CanSet() {
				continue
			}
			if err := r.resolveValue(f); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			if err := r.resolveValue(v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		for _, k := range v.MapKeys() {
			mv := v.MapIndex(k)
			cp := reflect.New(mv.Type()).Elem()
			cp.Set(mv)
			if err := r.resolveValue(cp); err != nil {
				return err
			}
			v.SetMapIndex(k, cp)
		}
		return nil
	case reflect.String:
		s := v.String()
		if !strings.Contains(s, "${") {
			return nil
		}
		resolved, err := r.ResolveString(s)
		if err != nil {
			return err
		}
		v.SetString(resolved)
		return nil
	default:
		return nil
	}
}

type placeholder struct {
	start, end int    // half-open byte range covering ${...}
	raw        string // the full ${...} text
	kind       string // env | file | vault
	body       string // text after "kind:"
}

// scanPlaceholders parses s into the placeholders it contains. It rejects
// nested placeholders (a `${` inside another placeholder body) and unknown
// kinds — these surface as a configuration error at startup.
func scanPlaceholders(s string) ([]placeholder, error) {
	var out []placeholder
	i := 0
	for i < len(s) {
		if i+1 >= len(s) || s[i] != '$' || s[i+1] != '{' {
			i++
			continue
		}
		start := i
		j := i + 2
		end := -1
		for j < len(s) {
			if s[j] == '}' {
				end = j
				break
			}
			if s[j] == '$' && j+1 < len(s) && s[j+1] == '{' {
				// don't echo s — the surrounding text may contain
				// a partially-typed secret that the user is still
				// resolving (a half-closed ${vault:...}).
				return nil, fmt.Errorf(
					"config: nested placeholder at offset %d (placeholders cannot be nested)",
					start,
				)
			}
			j++
		}
		if end < 0 {
			// same reason as above: do not embed the raw value in the error.
			return nil, fmt.Errorf("config: unclosed placeholder at offset %d (length %d)", start, len(s)-start)
		}
		raw := s[start : end+1]
		body := s[start+2 : end]
		colon := strings.IndexByte(body, ':')
		if colon < 0 {
			return nil, fmt.Errorf(
				"config: invalid placeholder %s: expected ${kind:body}", raw,
			)
		}
		kind := body[:colon]
		rest := body[colon+1:]
		switch kind {
		case placeholderEnv, placeholderFile, placeholderVault:
		default:
			return nil, fmt.Errorf(
				"config: unknown placeholder kind %q in %s (allowed: env, file, vault)",
				kind, raw,
			)
		}
		out = append(out, placeholder{start: start, end: end + 1, raw: raw, kind: kind, body: rest})
		i = end + 1
	}
	return out, nil
}

func (r Resolvers) resolveOne(p placeholder) (string, bool, error) {
	switch p.kind {
	case placeholderEnv:
		if r.Env == nil {
			return "", false, nil
		}
		return resolveEnv(p, r.Env)
	case placeholderFile:
		if r.File == nil {
			return "", false, nil
		}
		return resolveFile(p, r.File)
	case placeholderVault:
		if r.Vault == nil {
			return "", false, nil
		}
		return resolveVault(p, r.Vault)
	default:
		return "", false, fmt.Errorf("config: unsupported placeholder kind %q", p.kind)
	}
}

func resolveEnv(p placeholder, lookup EnvLookup) (string, bool, error) {
	name := p.body
	def := ""
	hasDefault := false
	if idx := strings.Index(name, ":-"); idx >= 0 {
		def = name[idx+2:]
		name = name[:idx]
		hasDefault = true
	}
	if !isValidEnvName(name) {
		return "", false, fmt.Errorf("config: invalid env var name %q in %s", name, p.raw)
	}
	v, present := lookup(name)
	switch {
	case present:
		return v, true, nil
	case hasDefault:
		return def, true, nil
	default:
		return "", false, fmt.Errorf("config: env var %q is not set and has no default in %s", name, p.raw)
	}
}

func resolveFile(p placeholder, read FileReader) (string, bool, error) {
	path := p.body
	if path == "" {
		return "", false, fmt.Errorf("config: empty file path in %s", p.raw)
	}
	data, err := read(path)
	if err != nil {
		return "", false, fmt.Errorf("config: read file for %s: %w", p.raw, err)
	}
	// trim a single trailing newline so files written by humans behave naturally
	return strings.TrimRight(string(data), "\r\n"), true, nil
}

func resolveVault(p placeholder, v VaultResolver) (string, bool, error) {
	path, key := p.body, ""
	if hash := strings.LastIndex(p.body, "#"); hash >= 0 {
		path = p.body[:hash]
		key = p.body[hash+1:]
	}
	if path == "" {
		return "", false, fmt.Errorf("config: empty vault path in %s", p.raw)
	}
	val, err := v.Lookup(path, key)
	if err != nil {
		return "", false, fmt.Errorf("config: vault lookup for %s: %w", p.raw, err)
	}
	return val, true, nil
}

func isValidEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		first := c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
		later := first || (c >= '0' && c <= '9')
		if i == 0 && !first {
			return false
		}
		if !later {
			return false
		}
	}
	return true
}

// assertNoPlaceholders is the final-phase guard: any remaining `${...}` in
// strings means an unresolved secret, which must be a hard error.
func assertNoPlaceholders(target any) error {
	rv := reflect.ValueOf(target)
	if !rv.IsValid() {
		return nil
	}
	return scanForPlaceholders(rv)
}

func scanForPlaceholders(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return scanForPlaceholders(v.Elem())
	case reflect.Struct:
		t := v.Type()
		for i := range v.NumField() {
			// honor the same placeholder:"-" opt-out as resolveValue: a
			// field resolved on the per-cluster path still carries raw
			// ${...} here and must not trip the completeness guard.
			if t.Field(i).Tag.Get("placeholder") == "-" {
				continue
			}
			if err := scanForPlaceholders(v.Field(i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			if err := scanForPlaceholders(v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		for _, k := range v.MapKeys() {
			if err := scanForPlaceholders(v.MapIndex(k)); err != nil {
				return err
			}
		}
		return nil
	case reflect.String:
		s := v.String()
		if strings.Contains(s, "${") {
			parts, err := scanPlaceholders(s)
			if err != nil {
				return err
			}
			if len(parts) > 0 {
				return fmt.Errorf("config: unresolved placeholder %s", parts[0].raw)
			}
		}
		return nil
	default:
		return nil
	}
}
