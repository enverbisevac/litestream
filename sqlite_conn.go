package litestream

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"modernc.org/sqlite"
)

// ConnectorFunc returns a connector for the SQLite database at path.
//
// Set DB.Connector or RestoreOptions.Connector when the application links its
// own SQLite build (e.g. SQLCipher via a mattn/go-sqlite3 fork) so Litestream
// opens the database through that build instead of modernc.org/sqlite. This is
// required for encrypted databases, and when embedding Litestream in a process
// that already has the database open, since two SQLite libraries in one process
// cannot see each other's POSIX locks.
type ConnectorFunc func(path string) (driver.Connector, error)

// NewDriverConnector returns a connector that opens dsn with drv. It adapts
// drivers that do not implement driver.DriverContext.
func NewDriverConnector(drv driver.Driver, dsn string) driver.Connector {
	if dc, ok := drv.(driver.DriverContext); ok {
		if c, err := dc.OpenConnector(dsn); err == nil {
			return c
		}
	}
	return &dsnConnector{driver: drv, dsn: dsn}
}

type dsnConnector struct {
	driver driver.Driver
	dsn    string
}

func (c *dsnConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}

func (c *dsnConnector) Driver() driver.Driver { return c.driver }

// pragmaConnector runs a fixed set of statements on every new connection.
// Litestream depends on per-connection settings such as busy_timeout and
// wal_autocheckpoint, which a caller-supplied DSN may not set.
type pragmaConnector struct {
	driver.Connector
	stmts []string
}

func (c *pragmaConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	for _, stmt := range c.stmts {
		if err := execConn(ctx, conn, stmt); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return conn, nil
}

func execConn(ctx context.Context, conn driver.Conn, query string) error {
	if execer, ok := conn.(driver.ExecerContext); ok {
		_, err := execer.ExecContext(ctx, query, nil)
		return err
	}
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	//lint:ignore SA1019 fallback for drivers without ExecerContext
	_, err = stmt.Exec(nil)
	return err
}

// openSQLDB opens the database at path through fn, or through the built-in
// modernc.org/sqlite driver if fn is nil. Each statement in stmts is executed
// on every new connection.
func openSQLDB(fn ConnectorFunc, path string, stmts ...string) (*sql.DB, error) {
	if fn == nil {
		return sql.Open("sqlite", path)
	}
	c, err := fn(path)
	if err != nil {
		return nil, fmt.Errorf("open connector: %w", err)
	}
	if len(stmts) == 0 {
		return sql.OpenDB(c), nil
	}
	return sql.OpenDB(&pragmaConnector{Connector: c, stmts: stmts}), nil
}

// sqliteFcntlPersistWAL is SQLITE_FCNTL_PERSIST_WAL.
const sqliteFcntlPersistWAL = 10

// setPersistWALConn enables SQLITE_FCNTL_PERSIST_WAL on a raw driver
// connection. It supports modernc.org/sqlite and mattn/go-sqlite3 (and forks).
func setPersistWALConn(driverConn any) error {
	switch fc := driverConn.(type) {
	case sqlite.FileControl:
		if _, err := fc.FileControlPersistWAL("main", 1); err != nil {
			return fmt.Errorf("FileControlPersistWAL: %w", err)
		}
		return nil
	case interface {
		SetFileControlInt(dbName string, op, arg int) error
	}:
		if err := fc.SetFileControlInt("main", sqliteFcntlPersistWAL, 1); err != nil {
			return fmt.Errorf("SetFileControlInt(PERSIST_WAL): %w", err)
		}
		return nil
	default:
		return fmt.Errorf("driver connection %T does not support file control", driverConn)
	}
}
