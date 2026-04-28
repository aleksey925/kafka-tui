package tui

import (
	"errors"
	"fmt"
	"strings"
)

// ScreenID identifies the high-level destinations the router knows how to
// jump to. New screens (added in later tasks) plug their handlers in via
// Router.Register.
type ScreenID string

const (
	ScreenClusters    ScreenID = "clusters"
	ScreenTopics      ScreenID = "topics"
	ScreenMessages    ScreenID = "messages"
	ScreenGroups      ScreenID = "groups"
	ScreenLogs        ScreenID = "logs"
	ScreenConfigSrc   ScreenID = "config-sources"
	ScreenHelpOverlay ScreenID = "help"
)

// Command is the parsed result of a `:` command bar entry.
type Command struct {
	Screen ScreenID
	// Arg is an optional positional argument: e.g. `:cluster <name>`.
	Arg string
	// Raw keeps the original input for logging.
	Raw string
}

// ErrUnknownCommand is returned by ParseCommand when the input does not match
// any known screen invocation.
var ErrUnknownCommand = errors.New("unknown command")

// ErrEmptyCommand is returned when the buffer is empty/whitespace.
var ErrEmptyCommand = errors.New("empty command")

// ParseCommand interprets `:topics`, `:groups`, `:clusters`, `:cluster <name>`,
// `:logs`, `:config sources`. The leading `:` may be present or absent.
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

// Router owns the screen-stack semantics. Screens are not yet implemented in
// this task — the router is intentionally a slim, observable container so
// later tasks can plug in real models without restructuring app.go.
type Router struct {
	stack []ScreenID
}

// NewRouter returns an empty router (no screens yet on the stack).
func NewRouter() *Router {
	return &Router{}
}

// Push appends id to the top of the stack and becomes the active screen.
func (r *Router) Push(id ScreenID) {
	r.stack = append(r.stack, id)
}

// Replace swaps the active screen for id, keeping stack depth equal. If the
// stack is empty Replace behaves like Push.
func (r *Router) Replace(id ScreenID) {
	if len(r.stack) == 0 {
		r.stack = append(r.stack, id)
		return
	}
	r.stack[len(r.stack)-1] = id
}

// Pop removes the top screen and returns the new active screen ID (empty
// when the stack is empty).
func (r *Router) Pop() ScreenID {
	if len(r.stack) == 0 {
		return ""
	}
	r.stack = r.stack[:len(r.stack)-1]
	return r.Active()
}

// Active returns the screen at the top of the stack (empty when empty).
func (r *Router) Active() ScreenID {
	if len(r.stack) == 0 {
		return ""
	}
	return r.stack[len(r.stack)-1]
}

// Depth returns the current stack depth (handy for tests and `Esc/q` logic).
func (r *Router) Depth() int {
	return len(r.stack)
}

// Stack returns a defensive copy of the current screen stack.
func (r *Router) Stack() []ScreenID {
	out := make([]ScreenID, len(r.stack))
	copy(out, r.stack)
	return out
}
