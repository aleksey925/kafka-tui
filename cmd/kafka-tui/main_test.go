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

func TestInlineClusterName_NoInline(t *testing.T) {
	assert.Empty(t, inlineClusterName(cli.CLICluster{}))
}

func TestInlineClusterName_ReturnsName(t *testing.T) {
	inline := cli.CLICluster{Name: "x-cli", Brokers: []string{"a:9092"}}

	assert.Equal(t, "x-cli", inlineClusterName(inline))
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

func TestMergeVaultConfig(t *testing.T) {
	tests := []struct {
		name  string
		yaml  config.VaultConfig
		flags *cli.Flags
		want  config.VaultConfig
	}{
		{
			name:  "no flags keeps yaml",
			yaml:  config.VaultConfig{Address: "https://yaml", Token: "y-tok"},
			flags: &cli.Flags{},
			want:  config.VaultConfig{Address: "https://yaml", Token: "y-tok"},
		},
		{
			name:  "address flag overrides yaml",
			yaml:  config.VaultConfig{Address: "https://yaml", Token: "y-tok"},
			flags: &cli.Flags{VaultAddr: "https://cli"},
			want:  config.VaultConfig{Address: "https://cli", Token: "y-tok"},
		},
		{
			name:  "token flag overrides yaml",
			yaml:  config.VaultConfig{Address: "https://yaml", Token: "y-tok"},
			flags: &cli.Flags{VaultToken: "c-tok"},
			want:  config.VaultConfig{Address: "https://yaml", Token: "c-tok"},
		},
		{
			name:  "both flags override yaml",
			yaml:  config.VaultConfig{Address: "https://yaml", Token: "y-tok"},
			flags: &cli.Flags{VaultAddr: "https://cli", VaultToken: "c-tok"},
			want:  config.VaultConfig{Address: "https://cli", Token: "c-tok"},
		},
		{
			name:  "whitespace-only flag does not blank a valid yaml value",
			yaml:  config.VaultConfig{Address: "https://yaml", Token: "y-tok"},
			flags: &cli.Flags{VaultAddr: "   ", VaultToken: "\t"},
			want:  config.VaultConfig{Address: "https://yaml", Token: "y-tok"},
		},
		{
			name:  "flags fill empty yaml",
			yaml:  config.VaultConfig{},
			flags: &cli.Flags{VaultAddr: "https://cli", VaultToken: "c-tok"},
			want:  config.VaultConfig{Address: "https://cli", Token: "c-tok"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeVaultConfig(tt.yaml, tt.flags)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewVaultResolver_RejectsSelfReferentialAddress(t *testing.T) {
	_, err := newVaultResolver(config.VaultConfig{
		Address: "${vault:secret/foo#addr}",
		Token:   "t",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault.address cannot itself be a ${vault:...}")
}

func TestNewVaultResolver_RejectsSelfReferentialToken(t *testing.T) {
	_, err := newVaultResolver(config.VaultConfig{
		Address: "https://vault.example.com",
		Token:   "${vault:secret/foo#token}",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault.token cannot itself be a ${vault:...}")
}

func TestResolveLogPath_FallbackToDefaultWhenLoadFails(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	flags := &cli.Flags{ConfigPath: filepath.Join(t.TempDir(), "missing-dir-that-does-not-exist")}

	path, err := resolveLogPath(flags)

	require.NoError(t, err)
	assert.NotEmpty(t, path)
	assert.True(t, filepath.IsAbs(path))
}
