package tui

import (
	"errors"
	"fmt"
	"strings"
)

// ScreenID identifies the high-level destinations the router knows how to
// jump to.
type ScreenID string

const (
	ScreenClusters     ScreenID = "clusters"
	ScreenTopics       ScreenID = "topics"
	ScreenTopicConfigs ScreenID = "topic-configs"
	ScreenMessages     ScreenID = "messages"
	ScreenProduce      ScreenID = "produce"
	ScreenGroups       ScreenID = "groups"
	ScreenLogs         ScreenID = "logs"
	ScreenConfigSrc    ScreenID = "config-sources"
	ScreenHelpOverlay  ScreenID = "help"
)

// Command is the parsed result of a `:` command bar entry.
type Command struct {
	Screen ScreenID
	// Arg is an optional positional argument: e.g. `:cluster <name>`.
	Arg string
	Raw string
}

// commandNames lists all commands recognized by ParseCommand. Compound
// commands appear as one entry so tab-completion completes the full phrase.
var commandNames = []string{
	"clusters",
	"config sources",
	"groups",
	"logs",
	"topics",
}

// CompletionSuggestion returns the first command starting with prefix, or
// "" when nothing matches. The returned string is always the full command name.
func CompletionSuggestion(prefix string) string {
	prefix = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(prefix), ":"))
	if prefix == "" {
		return ""
	}
	lower := strings.ToLower(prefix)
	for _, name := range commandNames {
		if strings.HasPrefix(name, lower) && name != lower {
			return name
		}
	}
	return ""
}

var ErrUnknownCommand = errors.New("unknown command")

var ErrEmptyCommand = errors.New("empty command")

// ParseCommand interprets `:topics`, `:groups`, `:clusters`, `:cluster <name>`,
// `:logs`, `:config sources`. The leading `:` is optional.
func ParseCommand(input string) (Command, error) {
	trimmed := strings.TrimSpace(input)
	trimmed = strings.TrimPrefix(trimmed, ":")
	if trimmed == "" {
		return Command{}, ErrEmptyCommand
	}

	fields := strings.Fields(trimmed)
	head := strings.ToLower(fields[0])
	rest := fields[1:]

	switch head {
	case "topics":
		if len(rest) > 0 {
			return Command{}, fmt.Errorf("%w: %q takes no arguments", ErrUnknownCommand, head)
		}
		return Command{Screen: ScreenTopics, Raw: trimmed}, nil
	case "groups":
		if len(rest) > 0 {
			return Command{}, fmt.Errorf("%w: %q takes no arguments", ErrUnknownCommand, head)
		}
		return Command{Screen: ScreenGroups, Raw: trimmed}, nil
	case "clusters":
		if len(rest) > 0 {
			return Command{}, fmt.Errorf("%w: %q takes no arguments", ErrUnknownCommand, head)
		}
		return Command{Screen: ScreenClusters, Raw: trimmed}, nil
	case "logs":
		if len(rest) > 0 {
			return Command{}, fmt.Errorf("%w: %q takes no arguments", ErrUnknownCommand, head)
		}
		return Command{Screen: ScreenLogs, Raw: trimmed}, nil
	case "cluster":
		if len(rest) != 1 {
			return Command{}, fmt.Errorf("%w: usage `:cluster <name>`", ErrUnknownCommand)
		}
		return Command{Screen: ScreenClusters, Arg: rest[0], Raw: trimmed}, nil
	case "config":
		if len(rest) == 1 && strings.EqualFold(rest[0], "sources") {
			return Command{Screen: ScreenConfigSrc, Raw: trimmed}, nil
		}
		return Command{}, fmt.Errorf("%w: usage `:config sources`", ErrUnknownCommand)
	default:
		return Command{}, fmt.Errorf("%w: %q", ErrUnknownCommand, head)
	}
}

// Router owns the screen-stack semantics.
type Router struct {
	stack []ScreenID
}

func NewRouter() *Router {
	return &Router{}
}

func (r *Router) Push(id ScreenID) {
	r.stack = append(r.stack, id)
}

// Replace swaps the active screen for id. On an empty stack it Pushes.
func (r *Router) Replace(id ScreenID) {
	if len(r.stack) == 0 {
		r.stack = append(r.stack, id)
		return
	}
	r.stack[len(r.stack)-1] = id
}

// Pop removes the top screen and returns the new active screen ID.
func (r *Router) Pop() ScreenID {
	if len(r.stack) == 0 {
		return ""
	}
	r.stack = r.stack[:len(r.stack)-1]
	return r.Active()
}

func (r *Router) Active() ScreenID {
	if len(r.stack) == 0 {
		return ""
	}
	return r.stack[len(r.stack)-1]
}

func (r *Router) Depth() int {
	return len(r.stack)
}

// Stack returns a defensive copy of the current screen stack.
func (r *Router) Stack() []ScreenID {
	out := make([]ScreenID, len(r.stack))
	copy(out, r.stack)
	return out
}
