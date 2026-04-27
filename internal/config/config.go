// Package config loads and merges kafka-tui YAML configuration files.
//
// Two layers are supported (low to high priority):
//   - global:  ~/.kafka-tui/{config,clusters}.yaml
//   - project: nearest .kafka-tui/{config,clusters}.yaml found by walking up
//     from the current working directory (similar to .git lookup)
//
// The optional --config flag points to either a single YAML file or a directory
// and disables hierarchy lookup for the corresponding file(s).
package config

// Config is the merged content of config.yaml.
type Config struct {
	Logging   LoggingConfig   `yaml:"logging"`
	Refresh   RefreshConfig   `yaml:"refresh"`
	Topics    TopicsConfig    `yaml:"topics"`
	Groups    GroupsConfig    `yaml:"groups"`
	Messages  MessagesConfig  `yaml:"messages"`
	Produce   ProduceConfig   `yaml:"produce"`
	Clipboard ClipboardConfig `yaml:"clipboard"`
	Vault     VaultConfig     `yaml:"vault"`
}

// LoggingConfig controls log destination, level, and rotation.
type LoggingConfig struct {
	Level     string `yaml:"level"`
	File      string `yaml:"file"`
	MaxSizeMB int    `yaml:"max_size_mb"`
	MaxFiles  int    `yaml:"max_files"`
}

// RefreshConfig holds auto-refresh intervals for each list view.
// Values are duration strings ("5s", "30s") or "off".
type RefreshConfig struct {
	TopicsList  string `yaml:"topics_list"`
	GroupsList  string `yaml:"groups_list"`
	GroupDetail string `yaml:"group_detail"`
}

// TopicsConfig controls the topics list screen.
type TopicsConfig struct {
	Columns []string `yaml:"columns"`
}

// GroupsConfig controls the consumer groups list screen.
type GroupsConfig struct {
	Columns []string `yaml:"columns"`
}

// MessagesConfig controls the messages list screen.
type MessagesConfig struct {
	Columns []string `yaml:"columns"`
}

// ProduceConfig controls the produce form behavior.
type ProduceConfig struct {
	HistorySize        int    `yaml:"history_size"`
	DefaultCompression string `yaml:"default_compression"`
}

// ClipboardConfig controls how copied text is exported.
type ClipboardConfig struct {
	Method string `yaml:"method"` // auto | native | osc52 | off
}

// VaultConfig holds Vault connection settings used by ${vault:...} placeholders.
type VaultConfig struct {
	Address string `yaml:"address"`
	Token   string `yaml:"token"`
}

// Defaults returns a Config populated with the documented default values.
func Defaults() Config {
	return Config{
		Logging: LoggingConfig{
			Level:     "info",
			File:      "~/.kafka-tui/kafka-tui.log",
			MaxSizeMB: 10,
			MaxFiles:  5,
		},
		Refresh: RefreshConfig{
			TopicsList:  "off",
			GroupsList:  "30s",
			GroupDetail: "5s",
		},
		Produce: ProduceConfig{
			HistorySize:        10,
			DefaultCompression: "none",
		},
		Clipboard: ClipboardConfig{
			Method: "auto",
		},
	}
}
