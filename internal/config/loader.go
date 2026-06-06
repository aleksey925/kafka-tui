package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

type Layer string

const (
	LayerGlobal   Layer = "global"
	LayerProject  Layer = "project"
	LayerExplicit Layer = "explicit" // --config override
)

const (
	DirName          = ".kafka-tui"
	ConfigFileName   = "config.yaml"
	ClustersFileName = "clusters.yaml"
)

type Source struct {
	Path  string
	Layer Layer
}

// Sources tracks the origin of every explicitly-set field after merging.
// Config keys are dotted paths like "logging.level". Cluster keys use the
// cluster name as the outer map key and dotted paths inside a cluster
// (e.g. "brokers", "sasl.username", "tls.ca_file").
type Sources struct {
	Config   map[string]Source
	Clusters map[string]map[string]Source
}

// ClusterContext returns a human-readable label of which configuration
// layers contributed at least one field to the named cluster. Single-layer
// clusters render as "global" / "project" / "explicit"; merged clusters
// render as "project + global" with project listed first because it
// overrides. An empty string means no provenance is tracked (typically
// the --brokers inline cluster), and the caller should fall back.
func ClusterContext(sources Sources, name string) string {
	fields := sources.Clusters[name]
	if len(fields) == 0 {
		return ""
	}
	seen := make(map[Layer]bool, 3)
	for _, src := range fields {
		seen[src.Layer] = true
	}
	parts := make([]string, 0, 3)
	for _, l := range []Layer{LayerProject, LayerGlobal, LayerExplicit} {
		if seen[l] {
			parts = append(parts, string(l))
		}
	}
	return strings.Join(parts, " + ")
}

type LoaderOptions struct {
	HomeDir    string
	StartDir   string
	ConfigPath string

	// VaultBuilder, when non-nil, is invoked from the lazy vault resolver
	// after the env+file phase. Returning (nil, nil) means "no vault
	// configured" — any remaining ${vault:...} placeholder then fails the
	// load via the final assertNoPlaceholders pass.
	//
	// Invariant: if the builder reads any field outside cfg.Vault (e.g. a
	// CLI flag captured by closure), the struct holding that field MUST be
	// present in ResolveTargets so it is materialized before the builder
	// runs. Otherwise the builder will read raw "${env:...}" / "${file:...}"
	// strings as if they were already-resolved values.
	VaultBuilder func(VaultConfig) (VaultResolver, error)

	// ResolveTargets are additional pointers (typically *cli.Flags) routed
	// through the same placeholder pipeline as the loaded YAML — see
	// CLAUDE.md § Placeholder pipeline. Targets are mutated in place and
	// frozen after the first Load.
	ResolveTargets []any

	// InlineCluster, when non-nil, is the CLI --brokers cluster with its
	// placeholders still unresolved. It is prepended to the loaded clusters
	// and runs through the same per-cluster pipeline, so a bad ${vault:...}
	// in --sasl-password quarantines it instead of aborting startup.
	InlineCluster *Cluster
}

type Loaded struct {
	Config Config
	// Clusters contains only clusters whose configuration was loaded
	// successfully. Existing consumers (UI selection, dial) operate on
	// these unchanged.
	Clusters []Cluster
	// InvalidClusters carries clusters whose config failed to load
	// (vault lookup failure, unresolved placeholder, TLS conflict, etc.)
	// One broken cluster must not abort startup — surfacing it here lets
	// the clusters screen render it as a non-selectable row with the
	// reason, while the rest of the app stays usable.
	InvalidClusters []InvalidCluster
	Sources         Sources
	Warnings        []string
}

// InvalidCluster pairs a partially-resolved cluster with the reason its
// load failed. The Cluster field carries whatever was materialized before
// the failure (typically the name + brokers + raw "${...}" strings on
// fields that didn't resolve) so the UI has something to display.
type InvalidCluster struct {
	Cluster Cluster
	Reason  error
}

// Load reads and merges configuration from all applicable layers.
func Load(opts LoaderOptions) (*Loaded, error) {
	if opts.HomeDir == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("config: cannot resolve home dir: %w", err)
		}
		opts.HomeDir = h
	}
	if opts.StartDir == "" {
		d, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("config: cannot resolve cwd: %w", err)
		}
		opts.StartDir = d
	}

	configFiles, clustersFiles, err := resolveFilePaths(opts)
	if err != nil {
		return nil, err
	}

	configMap := map[string]any{}
	var clustersList []any
	sources := Sources{
		Config:   map[string]Source{},
		Clusters: map[string]map[string]Source{},
	}

	for _, f := range configFiles {
		if mergeErr := readAndMergeConfigMap(configMap, sources.Config, f.Layer, f.Path); mergeErr != nil {
			return nil, mergeErr
		}
	}
	for _, f := range clustersFiles {
		updated, mergeErr := readAndMergeClustersMap(clustersList, sources.Clusters, f.Layer, f.Path)
		if mergeErr != nil {
			return nil, mergeErr
		}
		clustersList = updated
	}

	cfg := Defaults()
	if remarshalErr := remarshalInto(&cfg, configMap); remarshalErr != nil {
		return nil, fmt.Errorf("config: %w", remarshalErr)
	}

	var clusters []Cluster
	if len(clustersList) > 0 {
		var cf ClustersFile
		if remarshalErr := remarshalInto(&cf, map[string]any{"clusters": clustersList}); remarshalErr != nil {
			return nil, fmt.Errorf("config: %w", remarshalErr)
		}
		clusters = cf.Clusters
	}

	// must run before resolveGlobals / loadClusters — see CLAUDE.md §
	// Credential exposure warnings.
	credWarnings := checkYAMLCredentialExposure(cfg.Vault, clusters)

	envFile := EnvFileResolvers()
	vaultPhase, err := resolveGlobals(&cfg, opts.ResolveTargets, envFile, opts.VaultBuilder)
	if err != nil {
		return nil, err
	}

	// deep-copy so loadCluster resolves a clone, leaving opts.InlineCluster
	// with its placeholders intact — that is what lets a rotated secret be
	// picked up on reload (the inline cluster is re-resolved every Load).
	if opts.InlineCluster != nil {
		clusters = append([]Cluster{cloneCluster(*opts.InlineCluster)}, clusters...)
	}

	valid, invalid := loadClusters(clusters, envFile, vaultPhase)
	warnings := postProcessConfig(&cfg)
	warnings = append(warnings, postProcessClustersSoft(valid)...)
	warnings = append(warnings, credWarnings...)

	return &Loaded{
		Config:          cfg,
		Clusters:        valid,
		InvalidClusters: invalid,
		Sources:         sources,
		Warnings:        warnings,
	}, nil
}

// resolveGlobals runs env+file → vault → assert over cfg and CLI-supplied
// targets. Failures here are fatal because cfg drives global app state and
// CLI flags model explicit user intent — degrading would just defer the
// same error into confusing runtime failures. Returns the vault phase
// (with a lazy client built from the resolved cfg.Vault) for per-cluster
// reuse.
func resolveGlobals(cfg *Config, extras []any, envFile Resolvers, vb func(VaultConfig) (VaultResolver, error)) (Resolvers, error) {
	// env+file must run before the lazy vault client is built —
	// cfg.Vault.{Address,Token} themselves may carry ${env:...} /
	// ${file:...} placeholders that the client needs resolved to dial.
	if err := envFile.ResolveStruct(cfg); err != nil {
		return Resolvers{}, err
	}
	for _, t := range extras {
		if err := envFile.ResolveStruct(t); err != nil {
			return Resolvers{}, err
		}
	}

	// nil-Vault Resolvers passes through ${vault:...} placeholders; the
	// final assertNoPlaceholders then catches any leftovers as a hard
	// error here, or quarantines the offending cluster downstream.
	var vaultPhase Resolvers
	if vb != nil {
		vaultPhase = VaultOnlyResolvers(&lazyVaultResolver{vc: cfg.Vault, build: vb})
	}
	if err := vaultPhase.ResolveStruct(cfg); err != nil {
		return Resolvers{}, err
	}
	for _, t := range extras {
		if err := vaultPhase.ResolveStruct(t); err != nil {
			return Resolvers{}, err
		}
	}

	if err := assertNoPlaceholders(cfg); err != nil {
		return Resolvers{}, err
	}
	for _, t := range extras {
		if err := assertNoPlaceholders(t); err != nil {
			return Resolvers{}, err
		}
	}
	return vaultPhase, nil
}

// loadClusters runs each cluster through the same placeholder/validate
// pipeline independently. A failure quarantines only that cluster — the
// rest still load, so one bad config does not deny access to the rest.
func loadClusters(clusters []Cluster, envFile, vaultPhase Resolvers) (valid []Cluster, invalid []InvalidCluster) {
	valid = make([]Cluster, 0, len(clusters))
	invalid = make([]InvalidCluster, 0)
	for _, c := range clusters {
		if err := loadCluster(&c, envFile, vaultPhase); err != nil {
			invalid = append(invalid, InvalidCluster{Cluster: c, Reason: err})
			continue
		}
		valid = append(valid, c)
	}
	return valid, invalid
}

// cloneCluster deep-copies the fields loadCluster mutates in place (the
// brokers slice and the SASL / TLS pointers) so resolving the copy never
// touches the source. SASLConfig / TLSConfig hold only value fields, so a
// shallow struct copy behind a fresh pointer is a full clone.
func cloneCluster(c Cluster) Cluster {
	if c.Brokers != nil {
		c.Brokers = append([]string(nil), c.Brokers...)
	}
	if c.SASL != nil {
		sasl := *c.SASL
		c.SASL = &sasl
	}
	if c.TLS != nil {
		tls := *c.TLS
		c.TLS = &tls
	}
	return c
}

type fileSlot struct {
	Layer Layer
	Path  string
}

func resolveFilePaths(opts LoaderOptions) (configFiles, clustersFiles []fileSlot, err error) {
	var explicitConfig, explicitClusters string
	if opts.ConfigPath != "" {
		info, err := os.Stat(opts.ConfigPath)
		if err != nil {
			return nil, nil, fmt.Errorf("config: --config %q: %w", opts.ConfigPath, err)
		}
		if info.IsDir() {
			explicitConfig = filepath.Join(opts.ConfigPath, ConfigFileName)
			explicitClusters = filepath.Join(opts.ConfigPath, ClustersFileName)
		} else {
			switch filepath.Base(opts.ConfigPath) {
			case ConfigFileName:
				explicitConfig = opts.ConfigPath
			case ClustersFileName:
				explicitClusters = opts.ConfigPath
			default:
				return nil, nil, fmt.Errorf(
					"config: --config %q: file must be named %s or %s",
					opts.ConfigPath, ConfigFileName, ClustersFileName,
				)
			}
		}
	}

	if explicitConfig != "" {
		configFiles = []fileSlot{{LayerExplicit, explicitConfig}}
	} else {
		configFiles = hierarchyFiles(opts, ConfigFileName)
	}
	if explicitClusters != "" {
		clustersFiles = []fileSlot{{LayerExplicit, explicitClusters}}
	} else {
		clustersFiles = hierarchyFiles(opts, ClustersFileName)
	}
	return configFiles, clustersFiles, nil
}

func hierarchyFiles(opts LoaderOptions, name string) []fileSlot {
	result := []fileSlot{
		{LayerGlobal, filepath.Join(opts.HomeDir, DirName, name)},
	}
	if pd, ok := findProjectDir(opts.StartDir); ok {
		result = append(result, fileSlot{LayerProject, filepath.Join(pd, name)})
	}
	return result
}

// findProjectDir walks from startDir up the parent chain looking for a
// directory named DirName.
func findProjectDir(startDir string) (string, bool) {
	dir, err := filepath.Abs(startDir)
	if err != nil {
		return "", false
	}
	for {
		candidate := filepath.Join(dir, DirName)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func readAndMergeConfigMap(dst map[string]any, sources map[string]Source, layer Layer, path string) error {
	data, err := readYAMLFileIfExists(path)
	if err != nil || data == nil {
		return err
	}
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("config: parse %q: %w", path, err)
	}
	deepMergeMap(dst, raw, layer, path, "", sources)
	return nil
}

func readAndMergeClustersMap(
	dst []any,
	sources map[string]map[string]Source,
	layer Layer,
	path string,
) ([]any, error) {
	data, err := readYAMLFileIfExists(path)
	if err != nil || data == nil {
		return dst, err
	}
	var raw struct {
		Clusters []any `yaml:"clusters"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return dst, fmt.Errorf("config: parse %q: %w", path, err)
	}
	return mergeClustersList(dst, raw.Clusters, layer, path, sources)
}

func readYAMLFileIfExists(path string) ([]byte, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from user-supplied config locations
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}
	return data, nil
}

type lazyVaultResolver struct {
	vc    VaultConfig
	build func(VaultConfig) (VaultResolver, error)

	once  sync.Once
	inner VaultResolver
	err   error
}

func (l *lazyVaultResolver) Lookup(path, key string) (string, error) {
	l.once.Do(func() {
		l.inner, l.err = l.build(l.vc)
	})
	if l.err != nil {
		return "", l.err
	}
	if l.inner == nil {
		return "", errors.New("vault is not configured (set vault.address or --vault-addr)")
	}
	//nolint:wrapcheck // resolveVault wraps with "config: vault lookup for %s: %w" upstream.
	return l.inner.Lookup(path, key)
}

func remarshalInto(dst, src any) error {
	data, err := yaml.Marshal(src)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := yaml.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}
