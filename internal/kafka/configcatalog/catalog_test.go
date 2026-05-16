package configcatalog_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka/configcatalog"
)

func TestLookup__compressionType__select(t *testing.T) {
	// arrange
	const key = "compression.type"

	// act
	e, ok := configcatalog.Lookup(key)

	// assert
	require.True(t, ok)
	assert.Equal(t, key, e.Key)
	assert.Equal(t, "Compression", e.Category)
	assert.Equal(t, configcatalog.TypeSelect, e.Type)
	assert.Equal(t, []string{"uncompressed", "producer", "gzip", "lz4", "snappy", "zstd"}, e.EnumValues)
	assert.NotEmpty(t, e.Doc)
}

func TestLookup__cleanupPolicy__select(t *testing.T) {
	// act
	e, ok := configcatalog.Lookup("cleanup.policy")

	// assert
	require.True(t, ok)
	assert.Equal(t, configcatalog.TypeSelect, e.Type)
	assert.NotEmpty(t, e.EnumValues)
}

func TestLookup__retentionMs__duration(t *testing.T) {
	// act
	e, ok := configcatalog.Lookup("retention.ms")

	// assert
	require.True(t, ok)
	assert.Equal(t, "Retention", e.Category)
	assert.Equal(t, configcatalog.TypeDuration, e.Type)
}

func TestLookup__minInsyncReplicas__integer(t *testing.T) {
	// act
	e, ok := configcatalog.Lookup("min.insync.replicas")

	// assert
	require.True(t, ok)
	assert.Equal(t, "Replication", e.Category)
	assert.Equal(t, configcatalog.TypeInteger, e.Type)
}

func TestLookup__unknownKey__missing(t *testing.T) {
	// act
	_, ok := configcatalog.Lookup("does.not.exist")

	// assert
	assert.False(t, ok)
}

func TestAll__sortedAndComplete(t *testing.T) {
	// act
	all := configcatalog.All()

	// assert: 35 topic-level entries from the bundled snapshot
	require.Len(t, all, 35)

	// (category, key) ordering
	for i := 1; i < len(all); i++ {
		prev, cur := all[i-1], all[i]
		if prev.Category == cur.Category {
			assert.Less(t, prev.Key, cur.Key, "keys must be sorted within category")
		} else {
			assert.Less(t, prev.Category, cur.Category, "categories must be sorted")
		}
	}
}

func TestCategories__redpandaTaxonomy(t *testing.T) {
	// act
	cats := configcatalog.Categories()

	// assert
	assert.Equal(t, []string{
		"Compaction",
		"Compression",
		"Message Handling",
		"Replication",
		"Retention",
		"Storage Internals",
		"Write Caching",
	}, cats)
}

func TestCatalog__noResidualHTMLInDocs(t *testing.T) {
	// act / assert: the generator strips inline HTML; guard against regressions.
	for _, e := range configcatalog.All() {
		assert.NotContains(t, e.Doc, "<", "raw HTML left in doc for "+e.Key)
		assert.NotContains(t, e.Doc, "  ", "double-space artifacts in "+e.Key)
	}
}

func TestType_String(t *testing.T) {
	// act / assert
	cases := map[configcatalog.Type]string{
		configcatalog.TypeString:   "string",
		configcatalog.TypeInteger:  "integer",
		configcatalog.TypeBoolean:  "boolean",
		configcatalog.TypeSelect:   "select",
		configcatalog.TypeByteSize: "bytes",
		configcatalog.TypeDuration: "duration (ms)",
		configcatalog.TypeRatio:    "ratio",
	}
	for typ, label := range cases {
		assert.Equal(t, label, typ.String())
	}
}
