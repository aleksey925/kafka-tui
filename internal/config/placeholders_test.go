package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubVault struct {
	values map[string]string
	err    error
}

func (s *stubVault) Lookup(path, key string) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	k := path
	if key != "" {
		k = path + "#" + key
	}
	v, ok := s.values[k]
	if !ok {
		return "", errors.New("vault: not found: " + k)
	}
	return v, nil
}

func TestResolveString__envSet(t *testing.T) {
	// arrange
	t.Setenv("KT_USER", "alice")

	// act
	got, err := config.EnvFileResolvers().ResolveString("user=${env:KT_USER}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "user=alice", got)
}

func TestResolveString__envWithDefault__usesDefault(t *testing.T) {
	// arrange
	require.NoError(t, os.Unsetenv("KT_NOT_SET"))

	// act
	got, err := config.EnvFileResolvers().ResolveString("v=${env:KT_NOT_SET:-fallback}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "v=fallback", got)
}

func TestResolveString__envWithDefault__envWins(t *testing.T) {
	// arrange
	t.Setenv("KT_X", "real")

	// act
	got, err := config.EnvFileResolvers().ResolveString("v=${env:KT_X:-default}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "v=real", got)
}

func TestResolveString__envMissingNoDefault__error(t *testing.T) {
	// arrange
	require.NoError(t, os.Unsetenv("KT_REALLY_MISSING"))

	// act
	_, err := config.EnvFileResolvers().ResolveString("${env:KT_REALLY_MISSING}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KT_REALLY_MISSING")
}

func TestResolveString__file(t *testing.T) {
	// arrange
	dir := t.TempDir()
	secret := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(secret, []byte("s3cret\n"), 0o600))

	// act
	got, err := config.EnvFileResolvers().ResolveString("token=${file:" + secret + "}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "token=s3cret", got)
}

func TestResolveString__fileMissing__error(t *testing.T) {
	// arrange
	missing := filepath.Join(t.TempDir(), "no-such")

	// act
	_, err := config.EnvFileResolvers().ResolveString("${file:" + missing + "}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), missing)
}

func TestResolveString__fileEmptyPath__error(t *testing.T) {
	// act
	_, err := config.EnvFileResolvers().ResolveString("${file:}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty file path")
}

func TestResolveString__vaultWithKey(t *testing.T) {
	// arrange
	v := &stubVault{values: map[string]string{"secret/db#password": "p4ss"}}

	// act
	got, err := config.VaultOnlyResolvers(v).ResolveString("pw=${vault:secret/db#password}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "pw=p4ss", got)
}

func TestResolveString__vaultPathOnly(t *testing.T) {
	// arrange
	v := &stubVault{values: map[string]string{"secret/db": `{"password":"p4ss"}`}}

	// act
	got, err := config.VaultOnlyResolvers(v).ResolveString("blob=${vault:secret/db}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, `blob={"password":"p4ss"}`, got)
}

func TestResolveString__vaultLookupError(t *testing.T) {
	// arrange
	v := &stubVault{err: errors.New("boom")}

	// act
	_, err := config.VaultOnlyResolvers(v).ResolveString("${vault:secret/x}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestResolveString__multiplePlaceholders(t *testing.T) {
	// arrange
	t.Setenv("USER", "alice")
	t.Setenv("ENV", "prod")
	v := &stubVault{values: map[string]string{"k/p#token": "T"}}
	r := config.Resolvers{
		Env:   os.LookupEnv,
		Vault: v,
	}

	// act
	got, err := r.ResolveString("${env:USER}@${env:ENV}/${vault:k/p#token}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "alice@prod/T", got)
}

func TestResolveString__noPlaceholders__returnsInputUnchanged(t *testing.T) {
	// act
	got, err := config.EnvFileResolvers().ResolveString("plain string")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "plain string", got)
}

func TestResolveString__nestedPlaceholder__error(t *testing.T) {
	// act
	_, err := config.EnvFileResolvers().ResolveString("${env:${env:VAR}}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nested placeholder")
}

func TestResolveString__unclosedPlaceholder__error(t *testing.T) {
	// act
	_, err := config.EnvFileResolvers().ResolveString("${env:USER")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unclosed")
}

func TestResolveString__unknownKind__error(t *testing.T) {
	// act
	_, err := config.EnvFileResolvers().ResolveString("${secret:foo}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown placeholder kind")
}

func TestResolveString__missingColon__error(t *testing.T) {
	// act
	_, err := config.EnvFileResolvers().ResolveString("${envFOO}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected ${kind:body}")
}

func TestResolveString__invalidEnvName__error(t *testing.T) {
	// act
	_, err := config.EnvFileResolvers().ResolveString("${env:1BAD}")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid env var name")
}

func TestResolveString__phase1LeavesVaultIntact(t *testing.T) {
	// arrange
	t.Setenv("USER", "alice")

	// act — env+file resolver only; vault placeholder must survive untouched
	got, err := config.EnvFileResolvers().ResolveString("${env:USER}/${vault:secret/x#k}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "alice/${vault:secret/x#k}", got)
}

func TestResolveString__phase2LeavesEnvIntact(t *testing.T) {
	// arrange
	v := &stubVault{values: map[string]string{"secret/x#k": "T"}}

	// act — vault resolver only; env placeholder is left for phase 1 to handle
	got, err := config.VaultOnlyResolvers(v).ResolveString("${env:USER}/${vault:secret/x#k}")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "${env:USER}/T", got)
}

func TestResolveStruct__configStructWithEnvFileVault(t *testing.T) {
	// arrange
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, []byte("CADATA"), 0o600))
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("BROKER", "broker-1:9092")

	cfg := config.Config{
		Logging: config.LoggingConfig{Level: "${env:LOG_LEVEL}"},
	}
	clusters := []config.Cluster{
		{
			Name:    "prod",
			Brokers: []string{"${env:BROKER}", "static:9092"},
			SASL:    &config.SASLConfig{Mechanism: "PLAIN", Password: "${vault:kv/db#pw}"},
			TLS:     &config.TLSConfig{CA: "${file:" + caFile + "}"},
		},
	}
	v := &stubVault{values: map[string]string{"kv/db#pw": "p4ss"}}

	// act — phase 1
	require.NoError(t, config.EnvFileResolvers().ResolveStruct(&cfg))
	require.NoError(t, config.EnvFileResolvers().ResolveStruct(clusters))

	// act — phase 2
	require.NoError(t, config.VaultOnlyResolvers(v).ResolveStruct(clusters))

	// assert
	assert.Equal(t, "debug", cfg.Logging.Level)
	expectedClusters := []config.Cluster{
		{
			Name:    "prod",
			Brokers: []string{"broker-1:9092", "static:9092"},
			SASL:    &config.SASLConfig{Mechanism: "PLAIN", Password: "p4ss"},
			TLS:     &config.TLSConfig{CA: "CADATA"},
		},
	}
	assert.Equal(t, expectedClusters, clusters)
}

func TestResolveStruct__nilPointerSkipped(t *testing.T) {
	// arrange — Cluster.SASL pointer left nil; resolver must not panic
	clusters := []config.Cluster{{Name: "c", Brokers: []string{"b:9092"}}}

	// act
	err := config.EnvFileResolvers().ResolveStruct(clusters)

	// assert
	require.NoError(t, err)
}

func TestResolveAll__success(t *testing.T) {
	// arrange
	t.Setenv("PW", "p4ss")
	type creds struct {
		Token    string
		Password string
	}
	c := &creds{Token: "${env:PW}", Password: "${vault:k/p#pw}"}
	v := &stubVault{values: map[string]string{"k/p#pw": "vault-secret"}}

	// act
	err := config.ResolveAll(c, v)

	// assert
	require.NoError(t, err)
	assert.Equal(t, &creds{Token: "p4ss", Password: "vault-secret"}, c)
}

func TestResolveAll__noVaultButVaultPlaceholderRemains__error(t *testing.T) {
	// arrange
	type creds struct {
		Password string
	}
	c := &creds{Password: "${vault:k/p#pw}"}

	// act
	err := config.ResolveAll(c, nil)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unresolved placeholder")
}

func TestResolveAll__envFailureBeforeVault(t *testing.T) {
	// arrange
	require.NoError(t, os.Unsetenv("KT_NOPE"))
	type creds struct {
		V string
	}
	c := &creds{V: "${env:KT_NOPE}"}

	// act
	err := config.ResolveAll(c, &stubVault{})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KT_NOPE")
}

func TestLoad_PlaceholderResolution_EnvFile(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	cfgDir := filepath.Join(homeDir, ".kafka-tui")
	require.NoError(t, os.MkdirAll(cfgDir, 0o755))

	caFile := filepath.Join(homeDir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, []byte("CA-BLOB\n"), 0o600))

	t.Setenv("LOAD_LEVEL", "warn")
	t.Setenv("LOAD_BROKER", "broker:9092")

	cfgYAML := []byte("logging:\n  level: ${env:LOAD_LEVEL}\n")
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "config.yaml"), cfgYAML, 0o644))

	clustersYAML := []byte(
		"clusters:\n" +
			"  - name: prod\n" +
			"    brokers: [\"${env:LOAD_BROKER}\"]\n" +
			"    tls:\n" +
			"      ca: \"${file:" + caFile + "}\"\n",
	)
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "clusters.yaml"), clustersYAML, 0o644))

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.NoError(t, err)
	assert.Equal(t, "warn", loaded.Config.Logging.Level)
	require.Len(t, loaded.Clusters, 1)
	assert.Equal(t, []string{"broker:9092"}, loaded.Clusters[0].Brokers)
	require.NotNil(t, loaded.Clusters[0].TLS)
	assert.Equal(t, "CA-BLOB", loaded.Clusters[0].TLS.CA)
}

func TestLoad_PlaceholderResolution_VaultLeftWhenNoResolver(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	cfgDir := filepath.Join(homeDir, ".kafka-tui")
	require.NoError(t, os.MkdirAll(cfgDir, 0o755))

	clustersYAML := []byte(
		"clusters:\n" +
			"  - name: prod\n" +
			"    brokers: [\"b:9092\"]\n" +
			"    sasl:\n" +
			"      mechanism: PLAIN\n" +
			"      username: u\n" +
			"      password: \"${vault:kv/db#pw}\"\n",
	)
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "clusters.yaml"), clustersYAML, 0o644))

	// act — without vault resolver, vault placeholder is left intact
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.NoError(t, err)
	require.Len(t, loaded.Clusters, 1)
	require.NotNil(t, loaded.Clusters[0].SASL)
	assert.Equal(t, "${vault:kv/db#pw}", loaded.Clusters[0].SASL.Password)
}

func TestLoad_PlaceholderResolution_VaultRunsWhenResolverPresent(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	cfgDir := filepath.Join(homeDir, ".kafka-tui")
	require.NoError(t, os.MkdirAll(cfgDir, 0o755))

	clustersYAML := []byte(
		"clusters:\n" +
			"  - name: prod\n" +
			"    brokers: [\"b:9092\"]\n" +
			"    sasl:\n" +
			"      mechanism: PLAIN\n" +
			"      username: u\n" +
			"      password: \"${vault:kv/db#pw}\"\n",
	)
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "clusters.yaml"), clustersYAML, 0o644))

	v := &stubVault{values: map[string]string{"kv/db#pw": "p4ss"}}

	// act
	loaded, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
		Vault:    v,
	})

	// assert
	require.NoError(t, err)
	require.Len(t, loaded.Clusters, 1)
	require.NotNil(t, loaded.Clusters[0].SASL)
	assert.Equal(t, "p4ss", loaded.Clusters[0].SASL.Password)
}

func TestLoad_PlaceholderResolution_MissingEnv__loadError(t *testing.T) {
	// arrange
	require.NoError(t, os.Unsetenv("KT_LOAD_MISSING"))
	homeDir := t.TempDir()
	cfgDir := filepath.Join(homeDir, ".kafka-tui")
	require.NoError(t, os.MkdirAll(cfgDir, 0o755))

	cfgYAML := []byte("logging:\n  level: ${env:KT_LOAD_MISSING}\n")
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "config.yaml"), cfgYAML, 0o644))

	// act
	_, err := config.Load(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KT_LOAD_MISSING")
}
