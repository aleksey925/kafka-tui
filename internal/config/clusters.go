package config

type ClustersFile struct {
	Clusters []Cluster `yaml:"clusters"`
}

type Cluster struct {
	Name     string      `yaml:"name"`
	Brokers  []string    `yaml:"brokers"`
	Color    string      `yaml:"color,omitempty"`
	ReadOnly bool        `yaml:"read_only,omitempty"`
	SASL     *SASLConfig `yaml:"sasl,omitempty"`
	TLS      *TLSConfig  `yaml:"tls,omitempty"`
}

type SASLConfig struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// TLSConfig holds TLS settings for a cluster. Inline fields (CA / Cert /
// Key) and their *_file counterparts are mutually exclusive: specifying
// both inline content and a file path for the same key is a load error. An
// empty section (`tls: {}`) is valid and means TLS with system CAs and no
// client certificate.
type TLSConfig struct {
	CA         string `yaml:"ca,omitempty"`
	CAFile     string `yaml:"ca_file,omitempty"`
	Cert       string `yaml:"cert,omitempty"`
	CertFile   string `yaml:"cert_file,omitempty"`
	Key        string `yaml:"key,omitempty"`
	KeyFile    string `yaml:"key_file,omitempty"`
	SkipVerify bool   `yaml:"skip_verify,omitempty"`
}
