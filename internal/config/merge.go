package config

import "fmt"

// deepMergeMap overlays src on top of dst. Maps recurse, scalars and lists
// replace. For each leaf path, sources is updated with the layer/file that
// produced the final value.
func deepMergeMap(dst, src map[string]any, layer Layer, path, prefix string, sources map[string]Source) {
	for k, sv := range src {
		fpath := k
		if prefix != "" {
			fpath = prefix + "." + k
		}
		if svm, ok := sv.(map[string]any); ok {
			dvm, dok := dst[k].(map[string]any)
			if !dok {
				dvm = map[string]any{}
				dst[k] = dvm
			}
			deepMergeMap(dvm, svm, layer, path, fpath, sources)
			continue
		}
		dst[k] = sv
		sources[fpath] = Source{Path: path, Layer: layer}
	}
}

// mergeClustersList merges src cluster definitions into dst by cluster name.
// Per-cluster fields are merged with deepMergeMap so a project layer can override
// individual fields without redefining the whole cluster.
func mergeClustersList(
	dst, src []any,
	layer Layer,
	path string,
	sources map[string]map[string]Source,
) ([]any, error) {
	byName := map[string]int{}
	for i, d := range dst {
		if dm, ok := d.(map[string]any); ok {
			if name, ok := dm["name"].(string); ok {
				byName[name] = i
			}
		}
	}
	for idx, s := range src {
		sm, ok := s.(map[string]any)
		if !ok {
			return dst, fmt.Errorf("config: clusters[%d] in %s is not a mapping", idx, path)
		}
		name, _ := sm["name"].(string)
		if name == "" {
			return dst, fmt.Errorf("config: clusters[%d] in %s missing 'name'", idx, path)
		}
		if sources[name] == nil {
			sources[name] = map[string]Source{}
		}
		if i, ok := byName[name]; ok {
			dm := dst[i].(map[string]any)
			deepMergeMap(dm, sm, layer, path, "", sources[name])
			continue
		}
		newm := map[string]any{}
		dst = append(dst, newm)
		byName[name] = len(dst) - 1
		deepMergeMap(newm, sm, layer, path, "", sources[name])
	}
	return dst, nil
}

func validateClusterTLS(c Cluster) error {
	if c.TLS == nil {
		return nil
	}
	if c.TLS.CA != "" && c.TLS.CAFile != "" {
		return fmt.Errorf("config: cluster %q: tls.ca and tls.ca_file cannot both be set", c.Name)
	}
	if c.TLS.Cert != "" && c.TLS.CertFile != "" {
		return fmt.Errorf("config: cluster %q: tls.cert and tls.cert_file cannot both be set", c.Name)
	}
	if c.TLS.Key != "" && c.TLS.KeyFile != "" {
		return fmt.Errorf("config: cluster %q: tls.key and tls.key_file cannot both be set", c.Name)
	}
	// cert and key are always a pair — accepting one without the other
	// passes load but fails later at connect time with a confusing tls
	// error. mirror the CLI flag validator (cli/flags.go) here.
	hasCert := c.TLS.Cert != "" || c.TLS.CertFile != ""
	hasKey := c.TLS.Key != "" || c.TLS.KeyFile != ""
	if hasCert != hasKey {
		return fmt.Errorf("config: cluster %q: tls cert and key must be set together", c.Name)
	}
	return nil
}
