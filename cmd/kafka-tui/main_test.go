package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/cli"
	"github.com/aleksey925/kafka-tui/internal/config"
)

func TestBuildClusterList_NoInline(t *testing.T) {
	loaded := []config.Cluster{{Name: "alpha"}, {Name: "beta"}}

	got, cliName := buildClusterList(loaded, cli.CLICluster{})

	assert.Equal(t, loaded, got)
	assert.Empty(t, cliName)
}

func TestBuildClusterList_PrependsInline(t *testing.T) {
	loaded := []config.Cluster{{Name: "alpha"}}
	inline := cli.CLICluster{Name: "cli", Brokers: []string{"a:9092"}}

	got, cliName := buildClusterList(loaded, inline)

	assert.Equal(t, []config.Cluster{
		{Name: "cli", Brokers: []string{"a:9092"}},
		{Name: "alpha"},
	}, got)
	assert.Equal(t, "cli", cliName)
}

func TestCliInlineToCluster_Plain(t *testing.T) {
	c := cliInlineToCluster(cli.CLICluster{
		Name:    "p",
		Brokers: []string{"a:9092", "b:9092"},
		Color:   "red",
	})

	assert.Equal(t, "p", c.Name)
	assert.Equal(t, []string{"a:9092", "b:9092"}, c.Brokers)
	assert.Equal(t, "red", c.Color)
	assert.Nil(t, c.SASL)
	assert.Nil(t, c.TLS)
}

func TestCliInlineToCluster_PopulatesSASLOnlyWhenSet(t *testing.T) {
	c := cliInlineToCluster(cli.CLICluster{
		Name:          "p",
		Brokers:       []string{"a:9092"},
		SASLMechanism: "PLAIN",
		SASLUsername:  "u",
		SASLPassword:  "p",
	})

	require.NotNil(t, c.SASL)
	assert.Equal(t, &config.SASLConfig{
		Mechanism: "PLAIN", Username: "u", Password: "p",
	}, c.SASL)
	assert.Nil(t, c.TLS)
}

func TestCliInlineToCluster_PopulatesTLSWhenAnyTLSFlagSet(t *testing.T) {
	c := cliInlineToCluster(cli.CLICluster{
		Name:        "p",
		Brokers:     []string{"a:9092"},
		TLSEnabled:  true,
		TLSCAFile:   "/etc/ssl/ca.pem",
		TLSCertFile: "/etc/ssl/cert.pem",
		TLSKeyFile:  "/etc/ssl/key.pem",
	})

	require.NotNil(t, c.TLS)
	assert.Equal(t, &config.TLSConfig{
		CAFile: "/etc/ssl/ca.pem", CertFile: "/etc/ssl/cert.pem", KeyFile: "/etc/ssl/key.pem",
	}, c.TLS)
}

func TestConfigPaths_GlobalFromHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	global, _ := configPaths()

	assert.Equal(t, filepath.Join(home, config.DirName, config.ClustersFileName), global)
}

func TestFindProjectDir_FindsInCurrent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, config.DirName), 0o755))

	got, ok := findProjectDir(dir)

	require.True(t, ok)
	assert.Equal(t, filepath.Join(dir, config.DirName), got)
}

func TestFindProjectDir_WalksUp(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, config.DirName), 0o755))
	deep := filepath.Join(root, "a", "b", "c")
	require.NoError(t, os.MkdirAll(deep, 0o755))

	got, ok := findProjectDir(deep)

	require.True(t, ok)
	assert.Equal(t, filepath.Join(root, config.DirName), got)
}

func TestFindProjectDir_NotFoundReturnsFalse(t *testing.T) {
	dir := t.TempDir()

	_, ok := findProjectDir(dir)

	assert.False(t, ok)
}

func TestProduceHistory_NilStoreReturnsNil(t *testing.T) {
	assert.Nil(t, produceHistory(nil, 0, nil))
}

func TestResolveLogPath_FallbackToDefaultWhenLoadFails(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	flags := &cli.Flags{ConfigPath: filepath.Join(t.TempDir(), "missing-dir-that-does-not-exist")}

	path, err := resolveLogPath(flags)

	require.NoError(t, err)
	assert.NotEmpty(t, path)
	assert.True(t, filepath.IsAbs(path))
}
