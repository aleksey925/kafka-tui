package config_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/aleksey925/kafka-tui/internal/config"
)

func TestSecret_FormattingPathsRedact(t *testing.T) {
	s := config.Secret("hunter2")

	// every formatting verb the runtime might invoke must NOT print the
	// value. %s and %v are exercised through %+v / %#v on a wrapping struct
	// below to avoid the gocritic "redundantSprint" warning on the direct
	// fmt.Sprintf("%s", s) form (which prefers s.String()).
	wrapped := struct{ S config.Secret }{S: s}
	cases := []struct {
		name string
		out  string
	}{
		{"String", s.String()},
		{"GoString", s.GoString()},
		{"struct %+v", fmt.Sprintf("%+v", wrapped)},
		{"struct %#v", fmt.Sprintf("%#v", wrapped)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotContains(t, tc.out, "hunter2", "value leaked via %s", tc.name)
			assert.Contains(t, tc.out, config.RedactedMarker)
		})
	}
}

func TestSecret_LogValueRedacts(t *testing.T) {
	// arrange — a TextHandler captures whatever the formatter emits for the
	// attribute, so the assertion catches any handler-level leak.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	s := config.Secret("hunter2")

	// act — slog.Any routes through LogValuer.
	logger.LogAttrs(context.Background(), slog.LevelInfo, "msg", slog.Any("secret", s))

	// assert
	out := buf.String()
	assert.NotContains(t, out, "hunter2", "value leaked into slog output")
	assert.Contains(t, out, config.RedactedMarker)
}

func TestSecret_RevealReturnsUnderlying(t *testing.T) {
	s := config.Secret("hunter2")
	assert.Equal(t, "hunter2", s.Reveal())
}

func TestSecret_YAMLRoundTripPreservesValue(t *testing.T) {
	// arrange — the loader's remarshalInto pattern round-trips through
	// yaml.Marshal + Unmarshal. A MarshalYAML that redacted would erase
	// the secret before kafka/vault could use it.
	type holder struct {
		Token config.Secret `yaml:"token"`
	}
	src := holder{Token: "hunter2"}

	// act
	encoded, err := yaml.Marshal(src)
	require.NoError(t, err)

	var dst holder
	require.NoError(t, yaml.Unmarshal(encoded, &dst))

	// assert
	assert.Equal(t, "hunter2", dst.Token.Reveal(),
		"YAML round-trip must preserve the underlying value; "+
			"redaction belongs on formatting paths only")
}

func TestSecret_StructEmbeddingDoesNotLeak(t *testing.T) {
	// arrange — the common foot-gun: someone slog's an entire config
	// struct. The redacted field must dominate over the parent struct's
	// default formatting.
	type sasl struct {
		Username string
		Password config.Secret
	}
	s := sasl{Username: "svc", Password: "hunter2"}

	// act
	out := fmt.Sprintf("%+v", s)

	// assert
	assert.NotContains(t, out, "hunter2", "embedded Secret leaked through parent struct format")
	assert.Contains(t, out, "svc", "non-secret fields must still render")
	assert.Contains(t, out, config.RedactedMarker)
}

func TestSecret_EmptyRendersAsEmpty(t *testing.T) {
	// an empty Secret renders as empty (not as the marker) so an
	// operator reading `password=` in a log can tell "field unset"
	// apart from "field set, redacted".
	empty := config.Secret("")

	assert.Empty(t, empty.String())
	assert.NotContains(t, empty.String(), config.RedactedMarker)

	assert.Equal(t, `Secret("")`, empty.GoString())

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logger.LogAttrs(context.Background(), slog.LevelInfo, "msg", slog.Any("secret", empty))
	assert.NotContains(t, buf.String(), config.RedactedMarker)
}

func TestSecret_MarshalJSON(t *testing.T) {
	t.Run("non-empty redacts", func(t *testing.T) {
		// arrange — the threat is a debug-time
		// `json.MarshalIndent(loaded.Config, ...)` leaking the value.
		holder := struct {
			Token config.Secret `json:"token"`
		}{Token: "hunter2"}

		// act
		raw, err := json.Marshal(holder)

		// assert
		require.NoError(t, err)
		assert.JSONEq(t, `{"token":"[REDACTED]"}`, string(raw))
		assert.NotContains(t, string(raw), "hunter2")
	})

	t.Run("empty stays empty", func(t *testing.T) {
		// arrange
		holder := struct {
			Token config.Secret `json:"token"`
		}{Token: ""}

		// act
		raw, err := json.Marshal(holder)

		// assert
		require.NoError(t, err)
		assert.JSONEq(t, `{"token":""}`, string(raw))
	})
}
