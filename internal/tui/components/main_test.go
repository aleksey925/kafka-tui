package components_test

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain silences slog so toast warn/error mirroring (toast.go) does not
// flood test stderr. Production sets a real logger via slog.SetDefault in
// cmd/kafka-tui/main.go.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
