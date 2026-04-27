package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeepMergeMap_ScalarOverride(t *testing.T) {
	// arrange
	dst := map[string]any{"a": 1, "b": 2}
	src := map[string]any{"b": 99, "c": 3}
	sources := map[string]Source{}

	// act
	deepMergeMap(dst, src, LayerProject, "/p/config.yaml", "", sources)

	// assert
	assert.Equal(t, map[string]any{"a": 1, "b": 99, "c": 3}, dst)
	assert.Equal(t, Source{Path: "/p/config.yaml", Layer: LayerProject}, sources["b"])
	assert.Equal(t, Source{Path: "/p/config.yaml", Layer: LayerProject}, sources["c"])
	_, hasA := sources["a"]
	assert.False(t, hasA, "untouched key must not record a source")
}

func TestDeepMergeMap_NestedRecurse(t *testing.T) {
	// arrange
	dst := map[string]any{
		"logging": map[string]any{"level": "info", "file": "/g.log"},
	}
	src := map[string]any{
		"logging": map[string]any{"level": "debug"},
	}
	sources := map[string]Source{}

	// act
	deepMergeMap(dst, src, LayerProject, "/p.yaml", "", sources)

	// assert
	expected := map[string]any{
		"logging": map[string]any{"level": "debug", "file": "/g.log"},
	}
	assert.Equal(t, expected, dst)
	assert.Equal(t, LayerProject, sources["logging.level"].Layer)
	_, hasFile := sources["logging.file"]
	assert.False(t, hasFile)
}

func TestDeepMergeMap_ListReplaces(t *testing.T) {
	// arrange
	dst := map[string]any{"columns": []any{"a", "b", "c"}}
	src := map[string]any{"columns": []any{"x", "y"}}
	sources := map[string]Source{}

	// act
	deepMergeMap(dst, src, LayerProject, "/p.yaml", "", sources)

	// assert
	assert.Equal(t, []any{"x", "y"}, dst["columns"])
	assert.Equal(t, LayerProject, sources["columns"].Layer)
}

func TestMergeClustersList_AddsAndMerges(t *testing.T) {
	// arrange
	dst := []any{
		map[string]any{
			"name":    "prod",
			"brokers": []any{"b1"},
			"sasl":    map[string]any{"username": "u1"},
		},
	}
	src := []any{
		map[string]any{
			"name":  "prod",
			"color": "red",
			"sasl":  map[string]any{"password": "p1"},
		},
		map[string]any{
			"name":    "dev",
			"brokers": []any{"b2"},
		},
	}
	sources := map[string]map[string]Source{}

	// act
	merged, err := mergeClustersList(dst, src, LayerProject, "/p.yaml", sources)

	// assert
	require.NoError(t, err)
	assert.Len(t, merged, 2)

	prod := merged[0].(map[string]any)
	assert.Equal(t, "prod", prod["name"])
	assert.Equal(t, []any{"b1"}, prod["brokers"], "brokers from existing entry preserved")
	assert.Equal(t, "red", prod["color"])
	assert.Equal(t, map[string]any{"username": "u1", "password": "p1"}, prod["sasl"])

	dev := merged[1].(map[string]any)
	assert.Equal(t, "dev", dev["name"])

	assert.Equal(t, LayerProject, sources["prod"]["color"].Layer)
	assert.Equal(t, LayerProject, sources["prod"]["sasl.password"].Layer)
	assert.Equal(t, LayerProject, sources["dev"]["brokers"].Layer)
}

func TestMergeClustersList_RejectsMissingName(t *testing.T) {
	// arrange
	src := []any{map[string]any{"brokers": []any{"b1"}}}

	// act
	_, err := mergeClustersList(nil, src, LayerGlobal, "/g.yaml", map[string]map[string]Source{})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing 'name'")
}

func TestMergeClustersList_RejectsNonMapping(t *testing.T) {
	// arrange
	src := []any{"not a map"}

	// act
	_, err := mergeClustersList(nil, src, LayerGlobal, "/g.yaml", map[string]map[string]Source{})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a mapping")
}

func TestValidateClusterTLS(t *testing.T) {
	// arrange
	cases := []struct {
		name    string
		tls     *TLSConfig
		wantErr string
	}{
		{name: "nil section is fine", tls: nil},
		{name: "empty section is fine", tls: &TLSConfig{}},
		{name: "ca_file only", tls: &TLSConfig{CAFile: "/a"}},
		{name: "ca only", tls: &TLSConfig{CA: "X"}},
		{name: "ca + ca_file", tls: &TLSConfig{CA: "X", CAFile: "/a"}, wantErr: "tls.ca and tls.ca_file"},
		{name: "cert + cert_file", tls: &TLSConfig{Cert: "X", CertFile: "/c"}, wantErr: "tls.cert and tls.cert_file"},
		{name: "key + key_file", tls: &TLSConfig{Key: "X", KeyFile: "/k"}, wantErr: "tls.key and tls.key_file"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// act
			err := validateClusterTLS(Cluster{Name: "c", TLS: tc.tls})

			// assert
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
