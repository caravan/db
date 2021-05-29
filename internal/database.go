package internal

import (
	"fmt"

	"github.com/caravan/db/column"
	"github.com/caravan/db/database"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"

	radix "github.com/caravan/go-immutable-radix"
)

type (
	// dbInfo is the internal implementation of a Transactor
	dbInfo struct {
		sequence value.Key
		tables   prefix.Prefix
		data     *radix.Tree
	}

	dbTxr struct {
		*dbInfo
		txn transaction.Txn
	}
)

// Error messages
const (
	ErrTableAlreadyExists = "table already exists: %s"
)

// NewDatabase returns a new Transactor instance
func NewDatabase() database.Transactor {
	sequence := prefix.Start
	sequenceKey := sequence.Bytes()
	tables := sequence.Next()
	data, _, _ := radix.New().Insert(sequenceKey, tables)
	return newDatabaseTransactor(&dbInfo{
		sequence: sequenceKey,
		tables:   tables,
		data:     data,
	})
}

func newDatabaseTransactor(db *dbInfo) database.Transactor {
	return func(fn database.Query) (database.Transactor, error) {
		dbCopy := db.copy()
		txn := makeTransaction(dbCopy)
		err := fn(dbCopy.transactor(txn))
		if err != nil || !txn.commit() {
			return newDatabaseTransactor(db), err
		}
		return newDatabaseTransactor(dbCopy), nil
	}
}

func (db *dbInfo) copy() *dbInfo {
	return &(*db)
}

func (db *dbInfo) transactor(txn transaction.Txn) *dbTxr {
	return &dbTxr{
		dbInfo: db,
		txn:    txn,
	}
}

func (db *dbInfo) tableKey(n table.Name) value.Key {
	return db.tables.WithKey(value.Key(n))
}

func (db *dbTxr) Tables() table.Names {
	var res table.Names
	_ = db.txn.ForEach(db.tables, func(k value.Key, v transaction.Any) error {
		name := table.Name(k)
		res = append(res, name)
		return nil
	})
	return res
}

func (db *dbTxr) Table(n table.Name) (table.Table, bool) {
	if tbl, ok := db.txn.Get(db.tableKey(n)); ok {
		return tbl.(*tableInfo).transactor(db), true
	}
	return nil, false
}

func (db *dbTxr) CreateTable(
	n table.Name, cols ...column.Column,
) (table.Table, error) {
	key := db.tableKey(n)
	if _, ok := db.txn.Get(key); ok {
		return nil, fmt.Errorf(ErrTableAlreadyExists, n)
	}

	tbl := makeTable(db, n, cols...)
	db.txn.Insert(key, tbl)
	return tbl.transactor(db), nil
}

func (db *dbTxr) nextPrefix() prefix.Prefix {
	next := prefix.Start
	if stored, ok := db.txn.Get(db.sequence); ok {
		next = stored.(prefix.Prefix).Next()
	}
	db.txn.Insert(db.sequence, next)
	return next
}
