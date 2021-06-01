package internal

import (
	"fmt"

	"github.com/caravan/db/column"
	"github.com/caravan/db/database"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/transaction/iterate"
	"github.com/caravan/db/value"

	radix "github.com/caravan/go-immutable-radix"
)

type (
	// dbInfo is the internal implementation of a Transactor
	dbInfo struct {
		sequence prefix.Prefix
		tables   prefix.Prefix
		data     *radix.Tree
	}

	dbTxr struct {
		*dbInfo
		txn transaction.Txn
	}
)

var seqKey = value.Key("sequence")

// Error messages
const (
	ErrTableAlreadyExists = "table already exists: %s"
)

// NewDatabase returns a new Transactor instance
func NewDatabase() database.Transactor {
	sequence := prefix.Start
	tables := sequence.Next()
	data, _, _ := radix.New().Insert(sequence.WithKey(seqKey), tables)
	return newDatabaseTransactor(&dbInfo{
		sequence: sequence,
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

func (db *dbTxr) Tables() table.Names {
	var res table.Names
	_ = iterate.ForEach(db.txn.Ascending(db.tables).All(),
		func(k value.Key, v transaction.Any) error {
			name := table.Name(k)
			res = append(res, name)
			return nil
		},
	)
	return res
}

func (db *dbTxr) Table(n table.Name) (table.Table, bool) {
	if tbl, ok := db.txn.Get(db.tables, value.Key(n)); ok {
		return tbl.(*tableInfo).transactor(db), true
	}
	return nil, false
}

func (db *dbTxr) CreateTable(
	n table.Name, cols ...column.Column,
) (table.Table, error) {
	key := value.Key(n)
	if _, ok := db.txn.Get(db.tables, key); ok {
		return nil, fmt.Errorf(ErrTableAlreadyExists, n)
	}

	tbl := makeTable(db, n, cols...)
	db.txn.Insert(db.tables, key, tbl)
	return tbl.transactor(db), nil
}

func (db *dbTxr) nextPrefix() prefix.Prefix {
	next := prefix.Start
	if stored, ok := db.txn.Get(db.sequence, seqKey); ok {
		next = stored.(prefix.Prefix).Next()
	}
	db.txn.Insert(db.sequence, seqKey, next)
	return next
}
