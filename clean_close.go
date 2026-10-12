package litestream

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

// cleanCloseFilename is the record Close leaves in the meta directory after
// replicating the whole WAL and checkpointing it into the database file.
//
// Without it a restart always cost a full snapshot when Litestream runs inside
// the application that owns the database: the application's last connection
// closes after Litestream's, checkpoints, and drops the WAL, and on the next
// start verify cannot tell that from a WAL some other process overwrote.
const cleanCloseFilename = "clean-close.json"

// cleanCloseRecord is the database file's checksum at TXID, taken while
// Litestream held the write lock with every committed frame already in the
// file. A later start whose database file still hashes to it knows the file is
// exactly the replicated state, so any WAL it finds is new and can be synced
// incrementally.
type cleanCloseRecord struct {
	TXID     ltx.TXID     `json:"txid"`
	PageSize int          `json:"page_size"`
	Commit   uint32       `json:"commit"`
	Checksum ltx.Checksum `json:"checksum"`
}

func (db *DB) cleanClosePath() string {
	return filepath.Join(db.metaPath, cleanCloseFilename)
}

// recordCleanClose writes the clean-close record. It must run with execSem
// held, after the final sync. Any failure leaves no record, which only costs
// the next start the full snapshot it would have taken anyway.
func (db *DB) recordCleanClose(ctx context.Context) (err error) {
	if err := os.Remove(db.cleanClosePath()); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Hold the write lock from the seal until the hash is done: a commit in
	// between would land in the file, or the WAL, unreplicated.
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = rollback(tx) }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO _litestream_lock (id) VALUES (1);`); err != nil {
		return fmt.Errorf("_litestream_lock: %w", err)
	}

	exec, err := db.newSyncExecutor(ctx)
	if err != nil || exec == nil {
		return err
	}
	result, err := db.verifyAndSyncWithExecutor(ctx, false, exec, 0)
	if err != nil {
		db.applySyncExecutor(exec, false)
		return fmt.Errorf("seal: %w", err)
	}
	exec.applySyncResult(result)
	db.applySyncExecutor(exec, true)

	// Our own read lock would stop the checkpoint short of the WAL's end.
	if err := db.releaseReadLock(); err != nil {
		return fmt.Errorf("release read lock: %w", err)
	}
	var row [3]int
	if err := db.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE);`).Scan(&row[0], &row[1], &row[2]); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	if row[0] != 0 || row[1] != row[2] {
		db.Logger.Debug("clean close not recorded: wal not fully checkpointed", "result", fmt.Sprintf("%d,%d,%d", row[0], row[1], row[2]))
		return nil
	}

	pos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("pos: %w", err)
	}
	commit, err := db.ltxCommit(pos.TXID)
	if err != nil {
		return err
	}
	checksum, err := db.checksumDBFile(ctx, commit)
	if err != nil {
		return err
	}
	if err := rollback(tx); err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	return writeCleanCloseRecord(db.cleanClosePath(), cleanCloseRecord{
		TXID:     pos.TXID,
		PageSize: db.pageSize,
		Commit:   commit,
		Checksum: checksum,
	})
}

// takeCleanClose reads and removes the clean-close record, so it is trusted
// for at most one verify after a start.
func (db *DB) takeCleanClose() *cleanCloseRecord {
	path := db.cleanClosePath()
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	_ = os.Remove(path)
	var rec cleanCloseRecord
	if err := json.Unmarshal(buf, &rec); err != nil {
		return nil
	}
	return &rec
}

// resumeAfterCleanClose turns a verify that would snapshot into an
// incremental sync from the start of the current WAL, when rec proves the
// database file is exactly the state at the last LTX file. It requires a WAL
// generation other than the one that LTX file was read from: every frame in a
// new generation was written after the record, so none is already replicated.
func (db *DB) resumeAfterCleanClose(ctx context.Context, rec *cleanCloseRecord, exec *syncExecutor, info syncInfo) (syncInfo, bool) {
	if rec.TXID != exec.pos.TXID || rec.PageSize != db.pageSize {
		return info, false
	}
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return info, false
	}
	salt1, salt2 := binary.BigEndian.Uint32(hdr[16:]), binary.BigEndian.Uint32(hdr[20:])
	if salt1 == info.salt1 && salt2 == info.salt2 {
		return info, false
	}
	checksum, err := db.checksumDBFile(ctx, rec.Commit)
	if err != nil || checksum != rec.Checksum {
		db.Logger.Debug("clean close does not match database file", "error", err)
		return info, false
	}

	db.Logger.Log(ctx, internal.LevelTrace, "resuming after clean close",
		"txid", rec.TXID.String(),
		"skipped_reason", info.reason)
	return syncInfo{
		offset:              WALHeaderSize,
		salt1:               salt1,
		salt2:               salt2,
		prevCommit:          info.prevCommit,
		clearSyncedToWALEnd: true,
	}, true
}

// ltxCommit is the database size in pages after txID, from its L0 file.
func (db *DB) ltxCommit(txID ltx.TXID) (uint32, error) {
	f, err := os.Open(db.LTXPath(0, txID, txID))
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		return 0, fmt.Errorf("decode ltx header: %w", err)
	}
	return dec.Header().Commit, nil
}

var errCleanCloseSize = errors.New("database file size does not match commit")

// checksumDBFile hashes the database file's first commit pages. It reads
// through db.f: opening and closing another descriptor would drop every POSIX
// lock this process holds on the file, the application's included.
func (db *DB) checksumDBFile(ctx context.Context, commit uint32) (ltx.Checksum, error) {
	size := int64(commit) * int64(db.pageSize)
	fi, err := db.f.Stat()
	if err != nil {
		return 0, err
	}
	if fi.Size() != size {
		return 0, errCleanCloseSize
	}
	return ltx.ChecksumReader(&ctxReader{ctx: ctx, r: io.NewSectionReader(db.f, 0, size)}, db.pageSize)
}

func writeCleanCloseRecord(path string, rec cleanCloseRecord) error {
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *ctxReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
