package state

import (
	"context"
	"database/sql"
	"fmt"
)

// migration is a single forward-only schema step. Steps are applied in order
// and each one bumps `schema_version.version`. Never rewrite an applied
// migration — append a new entry instead.
type migration struct {
	version int
	stmts   []string
}

var migrations = []migration{
	{
		version: 1,
		stmts: []string{
			`CREATE TABLE IF NOT EXISTS schema_version (
				version INTEGER PRIMARY KEY
			)`,
			`CREATE TABLE IF NOT EXISTS produce_history (
				id          INTEGER PRIMARY KEY AUTOINCREMENT,
				cluster     TEXT    NOT NULL,
				topic       TEXT    NOT NULL,
				key         BLOB,
				value       BLOB,
				headers     TEXT    NOT NULL DEFAULT '[]',
				partition   INTEGER NOT NULL,
				compression TEXT    NOT NULL,
				ts          INTEGER NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS produce_history_ts_idx
				ON produce_history (ts DESC)`,
			`CREATE INDEX IF NOT EXISTS produce_history_cluster_topic_ts_idx
				ON produce_history (cluster, topic, ts DESC)`,
		},
	},
	{
		version: 2,
		stmts: []string{
			`CREATE TABLE IF NOT EXISTS messages_view_state (
				cluster_name TEXT    NOT NULL,
				topic        TEXT    NOT NULL,
				seek_mode    INTEGER NOT NULL,
				seek_params  TEXT    NOT NULL DEFAULT '{}',
				partitions   TEXT    NOT NULL DEFAULT '',
				updated_at   INTEGER NOT NULL,
				PRIMARY KEY (cluster_name, topic)
			)`,
		},
	},
	{
		version: 3,
		stmts: []string{
			`CREATE TABLE IF NOT EXISTS refresh_intervals (
				screen_id   TEXT    PRIMARY KEY,
				interval_ns INTEGER NOT NULL,
				updated_at  INTEGER NOT NULL
			)`,
		},
	},
}

// applyMigrations runs every migration newer than the recorded version. The
// first migration bootstraps `schema_version` itself, so the first read may
// return zero with no error.
func applyMigrations(ctx context.Context, db *sql.DB) error {
	current, err := currentSchemaVersion(ctx, db)
	if err != nil {
		return err
	}
	for _, mig := range migrations {
		if mig.version <= current {
			continue
		}
		if err := applyMigration(ctx, db, mig); err != nil {
			return fmt.Errorf("state: migration %d: %w", mig.version, err)
		}
	}
	return nil
}

// applyMigration runs one migration inside a transaction so a partial failure
// cannot leave the schema mid-step.
func applyMigration(ctx context.Context, db *sql.DB, mig migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, stmt := range mig.stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO schema_version (version) VALUES (?)`,
		mig.version,
	); err != nil {
		return fmt.Errorf("record version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// currentSchemaVersion returns the highest recorded version, or 0 if the
// schema_version table does not yet exist.
func currentSchemaVersion(ctx context.Context, db *sql.DB) (int, error) {
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'`,
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("state: probe schema_version: %w", err)
	}

	var version int
	err = db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM schema_version`,
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("state: read schema_version: %w", err)
	}
	return version, nil
}
