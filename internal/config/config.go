// Package config loads and merges kafka-tui YAML configuration files.
// Two layers are supported (low to high priority): global
// (~/.kafka-tui/) and project (nearest .kafka-tui/ walking up from cwd).
// The --config flag overrides hierarchy lookup for matching files.
package config

type Config struct {
	Logging   LoggingConfig   `yaml:"logging"`
	Topics    TopicsConfig    `yaml:"topics"`
	Groups    GroupsConfig    `yaml:"groups"`
	Messages  MessagesConfig  `yaml:"messages"`
	Produce   ProduceConfig   `yaml:"produce"`
	Clipboard ClipboardConfig `yaml:"clipboard"`
	Vault     VaultConfig     `yaml:"vault"`
}

type LoggingConfig struct {
	Level     string `yaml:"level"`
	File      string `yaml:"file"`
	MaxSizeMB int    `yaml:"max_size_mb"`
	MaxFiles  int    `yaml:"max_files"`
}

type TopicsConfig struct {
	Columns []string `yaml:"columns"`
}

type GroupsConfig struct {
	Columns []string `yaml:"columns"`
}

type MessagesConfig struct {
	Columns []string `yaml:"columns"`
}

type ProduceConfig struct {
	DefaultCompression string `yaml:"default_compression"`
}

type ClipboardConfig struct {
	Method string `yaml:"method"` // auto | native | osc52 | off
}

type VaultConfig struct {
	Address string `yaml:"address"`
	Token   Secret `yaml:"token"`
}

func Defaults() Config {
	return Config{
		Logging: LoggingConfig{
			Level:     "info",
			File:      "~/.kafka-tui/kafka-tui.log",
			MaxSizeMB: 10,
			MaxFiles:  5,
		},
		Produce: ProduceConfig{
			DefaultCompression: "none",
		},
		Clipboard: ClipboardConfig{
			Method: "auto",
		},
	}
}
