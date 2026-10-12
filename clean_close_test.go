package litestream_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// An application that embeds Litestream closes its own connections after
// Litestream's, and the last one checkpoints and drops the WAL. Every restart
// used to answer that with a full snapshot.
func TestDB_RestartAfterCleanCloseSyncsIncrementally(t *testing.T) {
	ctx := context.Background()
	db, sqldb := seedCleanCloseDB(t)
	path, client := db.Path(), db.Replica.Client

	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(db.MetaPath(), "clean-close.json")); err != nil {
		t.Fatalf("clean close not recorded: %v", err)
	}
	pos := mustPos(t, db)
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}

	sqldb = testingutil.MustOpenSQLDB(t, path)
	defer sqldb.Close()
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (randomblob(3000))`); err != nil {
		t.Fatal(err)
	}

	db2 := reopenCleanCloseDB(t, path, client)
	defer db2.Close(ctx)
	if err := db2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(db2.MetaPath(), "clean-close.json")); !os.IsNotExist(err) {
		t.Fatalf("clean close record not consumed: %v", err)
	}
	if size := l0BytesAfter(t, db2, pos.TXID); size == 0 || size > 64<<10 {
		t.Fatalf("restart wrote %d bytes of L0, want a small incremental file", size)
	}

	assertRestoredRows(t, db2, 301)
}

// A database file changed while Litestream was down is not the replicated
// state any more, so the restart must still snapshot.
func TestDB_RestartAfterCleanCloseSnapshotsWhenFileChanged(t *testing.T) {
	ctx := context.Background()
	db, sqldb := seedCleanCloseDB(t)
	path, client := db.Path(), db.Replica.Client

	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	pos := mustPos(t, db)
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (randomblob(3000))`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := reopenCleanCloseDB(t, path, client)
	defer db2.Close(ctx)
	if err := db2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if size := l0BytesAfter(t, db2, pos.TXID); size < 512<<10 {
		t.Fatalf("restart wrote %d bytes of L0, want a full snapshot", size)
	}

	assertRestoredRows(t, db2, 301)
}

func seedCleanCloseDB(t *testing.T) (*litestream.DB, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	db, sqldb := testingutil.MustOpenDBs(t)
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (x BLOB)`); err != nil {
		t.Fatal(err)
	}
	for range 300 {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t VALUES (randomblob(3000))`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	return db, sqldb
}

func reopenCleanCloseDB(t *testing.T, path string, client litestream.ReplicaClient) *litestream.DB {
	t.Helper()
	db := testingutil.NewDB(t, path)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	return db
}

func mustPos(t *testing.T, db *litestream.DB) ltx.Pos {
	t.Helper()
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	return pos
}

// l0BytesAfter sums the local L0 files written after txID.
func l0BytesAfter(t *testing.T, db *litestream.DB, txID ltx.TXID) int64 {
	t.Helper()
	entries, err := os.ReadDir(db.LTXLevelDir(0))
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, e := range entries {
		minTXID, _, err := ltx.ParseFilename(e.Name())
		if err != nil || minTXID <= txID {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		n += fi.Size()
	}
	return n
}

func assertRestoredRows(t *testing.T, db *litestream.DB, want int) {
	t.Helper()
	ctx := context.Background()
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: out}); err != nil {
		t.Fatal(err)
	}
	restored := testingutil.MustOpenSQLDB(t, out)
	defer restored.Close()
	var check string
	if err := restored.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&check); err != nil || check != "ok" {
		t.Fatalf("integrity_check = %q, %v", check, err)
	}
	var n int
	if err := restored.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&n); err != nil || n != want {
		t.Fatalf("restored %d rows (err %v), want %d", n, err, want)
	}
}
