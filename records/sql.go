package records

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
)

// IndexDB is an implementation of the Index interface backed by a SQL database.
type IndexDB struct {
	db          *sql.DB
	selectKeys  *sql.Stmt
	insertKey   *sql.Stmt
	deleteKey   *sql.Stmt
	dropKeys    *sql.Stmt
	dropOffsets *sql.Stmt
}

// OpenIndex opens a SQL database using sql.Open with the given driver and data
// source names.
//
// Currently, the package only supports the "sqlite" driver (or "sqlite3").
// No hard dependency is taken on a particular SQLite driver, applications are
// free to select the driver of their choosing, the only expectations is that it
// will register under the name "sqlite" or "sqlite3".
func OpenIndex(driverName, dataSourceName string) (*IndexDB, error) {
	switch driverName {
	case "sqlite", "sqlite3":
	default:
		return nil, fmt.Errorf("unsupported SQL driver for Kafka records index: %s", driverName)
	}
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return OpenIndexDB(context.Background(), db)
}

// OpenIndexDB is a lower-level API to open a records index backed by a SQL
// database intended to allow customization by providing a pre-open sql.DB
// instance.
func OpenIndexDB(ctx context.Context, db *sql.DB) (*IndexDB, error) {
	index := &IndexDB{db: db}
	if err := index.open(ctx, db); err != nil {
		index.Close()
		return nil, err
	}
	return index, nil
}

func (index *IndexDB) open(ctx context.Context, db *sql.DB) (err error) {
	const (
		createTable = `create table if not exists keys (
	key    blob    not null,
	offset integer not null,
	primary key (key)
)`
		createIndex = `create index if not exists offsets on keys (offset)`
		selectKeys  = `select key, offset from keys where key like ?`
		insertKey   = `insert into keys (key, offset) values (?, ?)`
		deleteKey   = `delete from keys where key = ?`
		dropKeys    = `delete from keys where key like ?`
		dropOffsets = `delete from keys where offset < ?`
	)
	if _, err := db.ExecContext(ctx, createTable); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, createIndex); err != nil {
		return err
	}
	index.selectKeys, err = db.PrepareContext(ctx, selectKeys)
	if err != nil {
		return err
	}
	index.insertKey, err = db.PrepareContext(ctx, insertKey)
	if err != nil {
		return err
	}
	index.deleteKey, err = db.PrepareContext(ctx, deleteKey)
	if err != nil {
		return err
	}
	index.dropKeys, err = db.PrepareContext(ctx, dropKeys)
	if err != nil {
		return err
	}
	index.dropOffsets, err = db.PrepareContext(ctx, dropOffsets)
	return err
}

func closeStmt(stmt *sql.Stmt) {
	if stmt != nil {
		stmt.Close()
	}
}

// Close closes the index database, releasing all system resources.
func (index *IndexDB) Close() error {
	closeStmt(index.selectKeys)
	closeStmt(index.insertKey)
	closeStmt(index.deleteKey)
	closeStmt(index.dropKeys)
	closeStmt(index.dropOffsets)
	return index.db.Close()
}

// Select satisfies the Index interface.
func (index *IndexDB) Select(ctx context.Context, prefix []byte) Select {
	r, err := index.selectKeys.QueryContext(ctx, like(prefix))
	if err != nil {
		return &errorIndexDB{err: err}
	} else {
		return &selectIndexDB{rows: r}
	}
}

// Update satisfies the Index interface.
func (index *IndexDB) Update(ctx context.Context) Update {
	if tx, err := index.db.BeginTx(ctx, nil); err != nil {
		return &errorIndexDB{err: err}
	} else {
		return &updateIndexDB{ctx: ctx, index: index, tx: tx}
	}
}

type selectIndexDB struct {
	rows   *sql.Rows
	err    error
	key    []byte
	offset int64
}

func (s *selectIndexDB) setError(err error) {
	if err != nil && s.err == nil {
		s.err = err
	}
}

func (s *selectIndexDB) Close() error {
	s.setError(s.rows.Close())
	s.setError(s.rows.Err())
	return s.err
}

func (s *selectIndexDB) Next() bool {
	if s.err != nil {
		return false
	}
	if !s.rows.Next() {
		return false
	}
	s.key = s.key[:0]
	s.offset = 0
	err := s.rows.Scan(&s.key, &s.offset)
	s.setError(err)
	return err == nil
}

func (s *selectIndexDB) Entry() (key []byte, offset int64) {
	return s.key, s.offset
}

type updateIndexDB struct {
	ctx         context.Context
	index       *IndexDB
	tx          *sql.Tx
	insertKey   *sql.Stmt
	deleteKey   *sql.Stmt
	dropKeys    *sql.Stmt
	dropOffsets *sql.Stmt
}

func (u *updateIndexDB) Close() (err error) {
	if u.tx != nil {
		err, u.tx = u.tx.Rollback(), nil
	}
	closeStmt(u.insertKey)
	closeStmt(u.deleteKey)
	closeStmt(u.dropKeys)
	closeStmt(u.dropOffsets)
	return err
}

func (u *updateIndexDB) Insert(key []byte, offset int64) error {
	if err := validateIndexDBKey("Insert", key); err != nil {
		return err
	}
	if u.tx == nil {
		return sql.ErrTxDone
	}
	if u.insertKey == nil {
		u.insertKey = u.tx.StmtContext(u.ctx, u.index.insertKey)
	}
	_, err := u.insertKey.ExecContext(u.ctx, key, offset)
	return err
}

func (u *updateIndexDB) Delete(key []byte) error {
	if err := validateIndexDBKey("Delete", key); err != nil {
		return err
	}
	if u.tx == nil {
		return sql.ErrTxDone
	}
	if u.deleteKey == nil {
		u.deleteKey = u.tx.StmtContext(u.ctx, u.index.deleteKey)
	}
	_, err := u.deleteKey.ExecContext(u.ctx, key)
	return err
}

func (u *updateIndexDB) DropKeys(prefix []byte) error {
	if err := validateIndexDBKey("DropKeys", prefix); err != nil {
		return err
	}
	if u.tx == nil {
		return sql.ErrTxDone
	}
	if u.dropKeys == nil {
		u.dropKeys = u.tx.StmtContext(u.ctx, u.index.dropKeys)
	}
	_, err := u.dropKeys.ExecContext(u.ctx, like(prefix))
	return err
}

func (u *updateIndexDB) DropOffsets(limit int64) error {
	if u.tx == nil {
		return sql.ErrTxDone
	}
	if u.dropOffsets == nil {
		u.dropOffsets = u.tx.StmtContext(u.ctx, u.index.dropOffsets)
	}
	_, err := u.dropOffsets.ExecContext(u.ctx, limit)
	return err
}

func (u *updateIndexDB) Commit() error {
	if u.tx == nil {
		return sql.ErrTxDone
	}
	err := u.tx.Commit()
	u.tx = nil
	return err
}

type errorIndexDB struct{ err error }

func (i *errorIndexDB) Close() error               { return i.err }
func (i *errorIndexDB) Insert([]byte, int64) error { return i.err }
func (i *errorIndexDB) Delete([]byte) error        { return i.err }
func (i *errorIndexDB) DropKeys([]byte) error      { return i.err }
func (i *errorIndexDB) DropOffsets(int64) error    { return i.err }
func (i *errorIndexDB) Commit() error              { return i.err }
func (i *errorIndexDB) Next() bool                 { return false }
func (i *errorIndexDB) Entry() ([]byte, int64)     { return nil, 0 }

func like(p []byte) []byte {
	return append(p[:len(p):len(p)], '%')
}

func validateIndexDBKey(method string, key []byte) error {
	if bytes.IndexByte(key, '%') >= 0 {
		return fmt.Errorf("kafka-go/records.(*IndexDB).%s(%q): valid keys cannot contain the '%%' character", method, key)
	}
	return nil
}
