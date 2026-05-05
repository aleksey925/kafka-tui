package clipboard

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

type nativeTool struct {
	name string
	args []string
}

type LookPath func(name string) (string, error)

type CommandRunner func(ctx context.Context, name string, args []string, stdin []byte) error

// NativeOptions configures [NewNative]. Production leaves all fields zero;
// tests populate them to swap out platform detection, tool discovery, and
// command execution.
type NativeOptions struct {
	GOOS     string
	Env      func(string) string
	LookPath LookPath
	Runner   CommandRunner
}

// Native shells out to a platform-specific CLI tool to write to the
// system clipboard.
type Native struct {
	goos     string
	env      func(string) string
	lookPath LookPath
	runner   CommandRunner
}

func NewNative(opts NativeOptions) *Native {
	goos := opts.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	envFn := opts.Env
	if envFn == nil {
		envFn = os.Getenv
	}
	lookPath := opts.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	runner := opts.Runner
	if runner == nil {
		runner = defaultRunner
	}
	return &Native{
		goos:     goos,
		env:      envFn,
		lookPath: lookPath,
		runner:   runner,
	}
}

func DefaultNative() *Native {
	return NewNative(NativeOptions{})
}

func (n *Native) Copy(ctx context.Context, payload string) error {
	tool, err := n.selectTool()
	if err != nil {
		return err
	}
	if err := n.runner(ctx, tool.name, tool.args, []byte(payload)); err != nil {
		return fmt.Errorf("clipboard: native %s: %w", tool.name, err)
	}
	return nil
}

// selectTool returns the highest-priority tool available on PATH for the
// current platform. The candidate order is:
//
//	darwin              → pbcopy
//	linux + Wayland     → wl-copy, then xclip, then xsel
//	linux (default)     → xclip, then xsel, then wl-copy
//	other unix          → xclip, xsel, wl-copy
func (n *Native) selectTool() (nativeTool, error) {
	candidates := n.candidates()
	for _, c := range candidates {
		if _, err := n.lookPath(c.name); err == nil {
			return c, nil
		}
	}
	names := make([]string, 0, len(candidates))
	for _, c := range candidates {
		names = append(names, c.name)
	}
	return nativeTool{}, fmt.Errorf("clipboard: no native clipboard tool found (looked for: %v)", names)
}

func (n *Native) candidates() []nativeTool {
	switch n.goos {
	case "darwin":
		return []nativeTool{{name: "pbcopy"}}
	case "windows":
		// Windows uses `clip.exe` which reads stdin verbatim.
		return []nativeTool{{name: "clip"}}
	case "linux":
		if n.env("WAYLAND_DISPLAY") != "" {
			return []nativeTool{
				{name: "wl-copy"},
				{name: "xclip", args: []string{"-selection", "clipboard"}},
				{name: "xsel", args: []string{"--clipboard", "--input"}},
			}
		}
		return []nativeTool{
			{name: "xclip", args: []string{"-selection", "clipboard"}},
			{name: "xsel", args: []string{"--clipboard", "--input"}},
			{name: "wl-copy"},
		}
	default:
		return []nativeTool{
			{name: "xclip", args: []string{"-selection", "clipboard"}},
			{name: "xsel", args: []string{"--clipboard", "--input"}},
			{name: "wl-copy"},
			{name: "pbcopy"},
		}
	}
}

func defaultRunner(ctx context.Context, name string, args []string, stdin []byte) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = bytes.NewReader(stdin)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if msg := bytes.TrimSpace(stderr.Bytes()); len(msg) > 0 {
			return fmt.Errorf("exec %s: %w: %s", name, err, string(msg))
		}
		return fmt.Errorf("exec %s: %w", name, err)
	}
	return nil
}
