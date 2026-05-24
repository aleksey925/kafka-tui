package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testdataGlobal       = "testdata/global"
	testdataProject      = "testdata/project"
	testdataExplicitDir  = "testdata/explicit_dir"
	testdataExplicitFile = "testdata/explicit_file"
	testdataTLSInvalid   = "testdata/tls_invalid"
)

func TestLoad_Defaults_NoFiles(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	startDir := t.TempDir()

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: startDir,
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, config.Defaults(), loaded.Config)
	assert.Empty(t, loaded.Clusters)
	assert.Empty(t, loaded.Sources.Config)
	assert.Empty(t, loaded.Sources.Clusters)
	assert.Empty(t, loaded.Warnings)
}

func TestLoad_GlobalOnly(t *testing.T) {
	// arrange
	homeDir := setupGlobalLayer(t)
	startDir := t.TempDir()

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: startDir,
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, "info", loaded.Config.Logging.Level)
	assert.Equal(t, "/tmp/global.log", loaded.Config.Logging.File)
	assert.Equal(t, 20, loaded.Config.Logging.MaxSizeMB)
	assert.Equal(t, 5, loaded.Config.Logging.MaxFiles, "default preserved when not set in YAML")
	assert.Equal(t, []string{"name", "partitions", "replicas"}, loaded.Config.Topics.Columns)
	assert.Len(t, loaded.Clusters, 2)

	prod := findCluster(t, loaded.Clusters, "prod")
	assert.Equal(t, []string{"prod-broker-1:9092", "prod-broker-2:9092"}, prod.Brokers)
	assert.Equal(t, "red", prod.Color)
	assert.True(t, prod.ReadOnly)
	require.NotNil(t, prod.SASL)
	assert.Equal(t, "PLAIN", prod.SASL.Mechanism)
	require.NotNil(t, prod.TLS)
	assert.Equal(t, "/etc/kafka/ca.pem", prod.TLS.CAFile)

	assert.Equal(t, config.LayerGlobal, loaded.Sources.Config["logging.level"].Layer)
	assert.Equal(t, config.LayerGlobal, loaded.Sources.Clusters["prod"]["brokers"].Layer)
}

func TestLoad_ProjectOverridesGlobal(t *testing.T) {
	// arrange
	homeDir := setupGlobalLayer(t)
	startDir := setupProjectLayer(t)

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: startDir,
	})

	// assert
	require.NoError(t, err)

	// scalars: project wins; absent scalars retain global
	assert.Equal(t, "debug", loaded.Config.Logging.Level)
	assert.Equal(t, "/tmp/global.log", loaded.Config.Logging.File)

	// list replaced (not concatenated)
	assert.Equal(t, []string{"name", "partitions"}, loaded.Config.Topics.Columns)

	// nested map merge: clipboard fully replaced, default_compression untouched
	assert.Equal(t, "osc52", loaded.Config.Clipboard.Method)
	assert.Equal(t, "gzip", loaded.Config.Produce.DefaultCompression)

	// clusters merged by name: prod kept brokers from global, color from project
	prod := findCluster(t, loaded.Clusters, "prod")
	assert.Equal(t, []string{"prod-broker-1:9092", "prod-broker-2:9092"}, prod.Brokers)
	assert.Equal(t, "green", prod.Color)
	assert.False(t, prod.ReadOnly)
	require.NotNil(t, prod.SASL)
	assert.Equal(t, "PLAIN", prod.SASL.Mechanism, "global value preserved")
	assert.Equal(t, "project-user", prod.SASL.Username, "project override")
	assert.Equal(t, "prod-pass", prod.SASL.Password, "global value preserved")

	// stage cluster only in global
	stage := findCluster(t, loaded.Clusters, "stage")
	assert.Equal(t, "yellow", stage.Color)

	// dev cluster only in project
	dev := findCluster(t, loaded.Clusters, "dev")
	assert.Equal(t, "gray", dev.Color)
	assert.Equal(t, []string{"dev-broker-1:9092", "dev-broker-2:9092"}, dev.Brokers)

	// provenance
	assert.Equal(t, config.LayerProject, loaded.Sources.Config["logging.level"].Layer)
	assert.Equal(t, config.LayerGlobal, loaded.Sources.Config["logging.file"].Layer)
	assert.Equal(t, config.LayerProject, loaded.Sources.Config["topics.columns"].Layer)
	assert.Equal(t, config.LayerGlobal, loaded.Sources.Clusters["prod"]["brokers"].Layer)
	assert.Equal(t, config.LayerProject, loaded.Sources.Clusters["prod"]["color"].Layer)
	assert.Equal(t, config.LayerProject, loaded.Sources.Clusters["prod"]["sasl.username"].Layer)
	assert.Equal(t, config.LayerGlobal, loaded.Sources.Clusters["prod"]["sasl.mechanism"].Layer)
	assert.Equal(t, config.LayerProject, loaded.Sources.Clusters["dev"]["brokers"].Layer)
}

func TestLoad_HierarchyWalkUp(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	startDir := setupProjectLayer(t)
	deeper := filepath.Join(startDir, "nested", "subdir")
	require.NoError(t, os.MkdirAll(deeper, 0o755))

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: deeper,
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, "debug", loaded.Config.Logging.Level)
	assert.NotEmpty(t, loaded.Clusters)
}

func TestLoad_ConfigPath_Directory(t *testing.T) {
	// arrange
	homeDir := setupGlobalLayer(t)
	startDir := t.TempDir()
	dir, err := filepath.Abs(testdataExplicitDir)
	require.NoError(t, err)

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:    homeDir,
		StartDir:   startDir,
		ConfigPath: dir,
	})

	// assert
	require.NoError(t, err)
	// hierarchy disabled for both files
	assert.Equal(t, "warn", loaded.Config.Logging.Level)
	assert.Equal(t, "/tmp/explicit.log", loaded.Config.Logging.File)
	assert.Empty(t, loaded.Config.Topics.Columns, "global columns must not be applied")
	assert.Len(t, loaded.Clusters, 1)
	assert.Equal(t, "explicit", loaded.Clusters[0].Name)
	assert.Equal(t, config.LayerExplicit, loaded.Sources.Config["logging.level"].Layer)
	assert.Equal(t, config.LayerExplicit, loaded.Sources.Clusters["explicit"]["brokers"].Layer)
}

func TestLoad_ConfigPath_File_DisablesOnlyConfigHierarchy(t *testing.T) {
	// arrange
	homeDir := setupGlobalLayer(t)
	startDir := t.TempDir()
	file, err := filepath.Abs(filepath.Join(testdataExplicitFile, "config.yaml"))
	require.NoError(t, err)

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:    homeDir,
		StartDir:   startDir,
		ConfigPath: file,
	})

	// assert
	require.NoError(t, err)
	// config from explicit file
	assert.Equal(t, "error", loaded.Config.Logging.Level)
	// clusters still loaded from global hierarchy
	assert.Len(t, loaded.Clusters, 2)
	prod := findCluster(t, loaded.Clusters, "prod")
	assert.Equal(t, "red", prod.Color)
	assert.Equal(t, config.LayerExplicit, loaded.Sources.Config["logging.level"].Layer)
	assert.Equal(t, config.LayerGlobal, loaded.Sources.Clusters["prod"]["brokers"].Layer)
}

func TestLoad_ConfigPath_InvalidFileName(t *testing.T) {
	// arrange
	tmp := t.TempDir()
	bad := filepath.Join(tmp, "settings.yaml")
	require.NoError(t, os.WriteFile(bad, []byte("logging: {level: info}\n"), 0o644))

	// act
	_, err := config.Load(config.LoaderOptions{
		HomeDir:    t.TempDir(),
		StartDir:   t.TempDir(),
		ConfigPath: bad,
	})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be named")
}

func TestLoad_ConfigPath_Missing(t *testing.T) {
	// arrange
	missing := filepath.Join(t.TempDir(), "missing.yaml")

	// act
	_, err := config.Load(config.LoaderOptions{
		HomeDir:    t.TempDir(),
		StartDir:   t.TempDir(),
		ConfigPath: missing,
	})

	// assert
	require.Error(t, err)
}

func TestLoad_TLSValidation_RejectsCAandCAFile(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	src, err := os.ReadFile(filepath.Join(testdataTLSInvalid, "clusters.yaml"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(homeDir, ".kafka-tui", "clusters.yaml"), src, 0o644))

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert — TLS conflict for one cluster quarantines that cluster,
	// the rest of the app still loads.
	require.NoError(t, err)
	assert.Empty(t, loaded.Clusters)
	require.Len(t, loaded.InvalidClusters, 1)
	assert.Contains(t, loaded.InvalidClusters[0].Reason.Error(), "tls.ca and tls.ca_file")
}

func TestLoad_TLSValidation_EmptyTLSObjectAllowed(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	yaml := []byte("clusters:\n  - name: c1\n    brokers: [b:9092]\n    tls: {}\n")
	require.NoError(t, os.WriteFile(filepath.Join(homeDir, ".kafka-tui", "clusters.yaml"), yaml, 0o644))

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.NoError(t, err)
	require.Len(t, loaded.Clusters, 1)
	require.NotNil(t, loaded.Clusters[0].TLS)
	assert.Empty(t, loaded.Clusters[0].TLS.CAFile)
}

func TestLoad_MissingClusterName_Errors(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	yaml := []byte("clusters:\n  - brokers: [b:9092]\n")
	require.NoError(t, os.WriteFile(filepath.Join(homeDir, ".kafka-tui", "clusters.yaml"), yaml, 0o644))

	// act
	_, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing 'name'")
}

func TestLoad_PartialFailure_OneClusterBroken_OthersLoad(t *testing.T) {
	// arrange — mix a clean cluster with one that has a TLS conflict
	// (ca and ca_file both set). The clean cluster must still load.
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	yaml := []byte(`clusters:
  - name: clean
    brokers: [b1:9092]
  - name: broken
    brokers: [b2:9092]
    tls:
      ca: "INLINE"
      ca_file: /etc/ca.pem
  - name: clean2
    brokers: [b3:9092]
`)
	require.NoError(t, os.WriteFile(filepath.Join(homeDir, ".kafka-tui", "clusters.yaml"), yaml, 0o644))

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.NoError(t, err)
	require.Len(t, loaded.Clusters, 2)
	assert.Equal(t, "clean", loaded.Clusters[0].Name)
	assert.Equal(t, "clean2", loaded.Clusters[1].Name)
	require.Len(t, loaded.InvalidClusters, 1)
	assert.Equal(t, "broken", loaded.InvalidClusters[0].Cluster.Name)
	assert.Contains(t, loaded.InvalidClusters[0].Reason.Error(), "tls.ca and tls.ca_file")
}

func TestLoad_PartialFailure_OneClusterUnresolvedEnv_OthersLoad(t *testing.T) {
	// arrange — env-placeholder failure for one cluster (env var not set,
	// no default) must quarantine only that cluster.
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	yaml := []byte(`clusters:
  - name: clean
    brokers: [b1:9092]
  - name: needs-env
    brokers: [b2:9092]
    sasl:
      mechanism: PLAIN
      username: u
      password: "${env:KAFKA_TUI_TEST_NOT_SET_VAR}"
`)
	require.NoError(t, os.WriteFile(filepath.Join(homeDir, ".kafka-tui", "clusters.yaml"), yaml, 0o644))

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.NoError(t, err)
	require.Len(t, loaded.Clusters, 1)
	assert.Equal(t, "clean", loaded.Clusters[0].Name)
	require.Len(t, loaded.InvalidClusters, 1)
	assert.Equal(t, "needs-env", loaded.InvalidClusters[0].Cluster.Name)
	assert.Contains(t, loaded.InvalidClusters[0].Reason.Error(), "KAFKA_TUI_TEST_NOT_SET_VAR")
}

func TestLoad_LiteralCredentialsInYAML_SurfaceAsWarnings(t *testing.T) {
	// arrange — clusters.yaml with a literal sasl.password; config.yaml
	// with a literal vault.token. Both must be flagged via loaded.Warnings.
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	clustersYAML := []byte(`clusters:
  - name: leaky
    brokers: [b1:9092]
    sasl:
      mechanism: PLAIN
      username: u
      password: hunter2
  - name: safe
    brokers: [b2:9092]
    sasl:
      mechanism: PLAIN
      username: u
      password: "${env:KAFKA_TUI_TEST_NOT_SET_VAR:-fallback}"
`)
	require.NoError(t, os.WriteFile(filepath.Join(homeDir, ".kafka-tui", "clusters.yaml"), clustersYAML, 0o644))
	configYAML := []byte("vault:\n  token: s.deadbeef\n")
	require.NoError(t, os.WriteFile(filepath.Join(homeDir, ".kafka-tui", "config.yaml"), configYAML, 0o644))

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.NoError(t, err)
	require.Len(t, loaded.Clusters, 2)
	joined := strings.Join(loaded.Warnings, "\n")
	assert.Contains(t, joined, `cluster "leaky"`)
	assert.Contains(t, joined, "sasl.password")
	assert.Contains(t, joined, "vault.token")
	assert.NotContains(t, joined, `cluster "safe"`,
		"placeholder-resolved password must not produce a warning")
}

func TestClusterContext_UnknownCluster(t *testing.T) {
	got := config.ClusterContext(config.Sources{}, "missing")
	assert.Empty(t, got)
}

func TestClusterContext_SingleLayer(t *testing.T) {
	cases := []struct {
		layer    config.Layer
		expected string
	}{
		{config.LayerGlobal, "global"},
		{config.LayerProject, "project"},
		{config.LayerExplicit, "explicit"},
	}
	for _, tc := range cases {
		t.Run(string(tc.layer), func(t *testing.T) {
			sources := config.Sources{
				Clusters: map[string]map[string]config.Source{
					"prod": {
						"brokers":       {Layer: tc.layer, Path: "x.yaml"},
						"sasl.username": {Layer: tc.layer, Path: "x.yaml"},
					},
				},
			}
			assert.Equal(t, tc.expected, config.ClusterContext(sources, "prod"))
		})
	}
}

func TestClusterContext_MergedProjectOverGlobalListsProjectFirst(t *testing.T) {
	sources := config.Sources{
		Clusters: map[string]map[string]config.Source{
			"prod": {
				"brokers":       {Layer: config.LayerGlobal, Path: "g.yaml"},
				"color":         {Layer: config.LayerProject, Path: "p.yaml"},
				"sasl.username": {Layer: config.LayerProject, Path: "p.yaml"},
			},
		},
	}
	assert.Equal(t, "project + global", config.ClusterContext(sources, "prod"))
}

// helpers

func setupGlobalLayer(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	copyTestdataDir(t, testdataGlobal, filepath.Join(home, ".kafka-tui"))
	return home
}

func setupProjectLayer(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	copyTestdataDir(t, testdataProject, filepath.Join(root, ".kafka-tui"))
	return root
}

// copyTestdataDir copies all files from a testdata directory into dst.
// Used to seed temp homedirs / project roots for loader tests.
func copyTestdataDir(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dst, 0o755))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name())) //nolint:gosec // testdata only
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644))
	}
}

func findCluster(t *testing.T, clusters []config.Cluster, name string) config.Cluster {
	t.Helper()
	for _, c := range clusters {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("cluster %q not found", name)
	return config.Cluster{}
}
