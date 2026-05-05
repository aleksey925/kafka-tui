// Package config loads and merges kafka-tui YAML configuration files.
// Two layers are supported (low to high priority): global
// (~/.kafka-tui/) and project (nearest .kafka-tui/ walking up from cwd).
// The --config flag overrides hierarchy lookup for matching files.
package config

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

type LoggingConfig struct {
	Level     string `yaml:"level"`
	File      string `yaml:"file"`
	MaxSizeMB int    `yaml:"max_size_mb"`
	MaxFiles  int    `yaml:"max_files"`
}

// RefreshConfig values are duration strings ("5s", "30s") or "off".
type RefreshConfig struct {
	TopicsList  string `yaml:"topics_list"`
	GroupsList  string `yaml:"groups_list"`
	GroupDetail string `yaml:"group_detail"`
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
	HistorySize        int    `yaml:"history_size"`
	DefaultCompression string `yaml:"default_compression"`
}

type ClipboardConfig struct {
	Method string `yaml:"method"` // auto | native | osc52 | off
}

type VaultConfig struct {
	Address string `yaml:"address"`
	Token   string `yaml:"token"`
}

func Defaults() Config {
	return Config{
		Logging: LoggingConfig{
			Level:     "info",
			File:      "~/.kafka-tui/kafka-tui.log",
			MaxSizeMB: 10,
			MaxFiles:  5,
		},
		Refresh: RefreshConfig{
			TopicsList:  "30s",
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
