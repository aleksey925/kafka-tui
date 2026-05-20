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
	HomeDir        string
	StartDir       string
	ConfigPath     string
	CLIClusterName string

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
}

type Loaded struct {
	Config   Config
	Clusters []Cluster
	Sources  Sources
	Warnings []string
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

	if resolveErr := resolvePlaceholders(&cfg, clusters, opts.ResolveTargets, opts.VaultBuilder); resolveErr != nil {
		return nil, resolveErr
	}

	for _, c := range clusters {
		if validateErr := validateClusterTLS(c); validateErr != nil {
			return nil, validateErr
		}
	}

	var warnings []string
	if opts.CLIClusterName != "" {
		for i, c := range clusters {
			if c.Name == opts.CLIClusterName {
				warnings = append(warnings, fmt.Sprintf(
					"cluster %q from clusters.yaml is overridden by --brokers and excluded from this session",
					opts.CLIClusterName,
				))
				clusters = append(clusters[:i], clusters[i+1:]...)
				delete(sources.Clusters, opts.CLIClusterName)
				break
			}
		}
	}

	return &Loaded{
		Config:   cfg,
		Clusters: clusters,
		Sources:  sources,
		Warnings: warnings,
	}, nil
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

// resolvePlaceholders runs the placeholder pipeline (env+file → vault →
// assert) over cfg, clusters, and any extra targets. Phase order is
// load-bearing; see CLAUDE.md § Placeholder pipeline.
func resolvePlaceholders(
	cfg *Config,
	clusters []Cluster,
	extras []any,
	buildVault func(VaultConfig) (VaultResolver, error),
) error {
	envFile := EnvFileResolvers()
	if err := envFile.ResolveStruct(cfg); err != nil {
		return err
	}
	if err := envFile.ResolveStruct(clusters); err != nil {
		return err
	}
	for _, t := range extras {
		if err := envFile.ResolveStruct(t); err != nil {
			return err
		}
	}
	if err := resolveVaultPhase(cfg, clusters, extras, buildVault); err != nil {
		return err
	}
	if err := assertNoPlaceholders(cfg); err != nil {
		return err
	}
	if err := assertNoPlaceholders(clusters); err != nil {
		return err
	}
	for _, t := range extras {
		if err := assertNoPlaceholders(t); err != nil {
			return err
		}
	}
	return nil
}

func resolveVaultPhase(
	cfg *Config,
	clusters []Cluster,
	extras []any,
	buildVault func(VaultConfig) (VaultResolver, error),
) error {
	if buildVault == nil {
		return nil
	}
	// lazy: builder fires only on the first ${vault:...} encounter
	// (CLAUDE.md § Placeholder pipeline).
	lazy := &lazyVaultResolver{vc: cfg.Vault, build: buildVault}
	vaultPhase := VaultOnlyResolvers(lazy)
	if err := vaultPhase.ResolveStruct(cfg); err != nil {
		return err
	}
	if err := vaultPhase.ResolveStruct(clusters); err != nil {
		return err
	}
	for _, t := range extras {
		if err := vaultPhase.ResolveStruct(t); err != nil {
			return err
		}
	}
	return nil
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
