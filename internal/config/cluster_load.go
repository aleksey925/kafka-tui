package config

import "fmt"

// loadCluster runs the per-cluster placeholder/validate pipeline. Returns
// a non-nil error describing the failed step when the cluster is not
// usable; the caller routes such clusters into Loaded.InvalidClusters
// so the rest of the app still starts.
//
// The returned error intentionally does NOT include the cluster name —
// the caller (UI / slog) already has it via the surrounding
// InvalidCluster.Cluster.Name and adds it once. Wrapping it again here
// would produce "cluster X: cluster X: ..." in toasts.
//
// The cluster is mutated in place up to the point of failure. That's
// fine because the loader iterates over copies and only commits the
// value to Loaded.Clusters when this returns nil — a partially-resolved
// Cluster only ever ends up in InvalidClusters, where the original
// strings are useful for diagnostics.
func loadCluster(c *Cluster, envFile, vault Resolvers) error {
	if err := envFile.ResolveStruct(c); err != nil {
		return fmt.Errorf("env/file resolution: %w", err)
	}
	if err := vault.ResolveStruct(c); err != nil {
		return fmt.Errorf("vault resolution: %w", err)
	}
	if err := assertNoPlaceholders(c); err != nil {
		return fmt.Errorf("unresolved placeholder: %w", err)
	}
	if err := validateClusterTLS(*c); err != nil {
		return err
	}
	return normalizeClusterHard(c)
}
