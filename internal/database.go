package internal

import (
	"fmt"

	"github.com/caravan/db/column"
	"github.com/caravan/db/database"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"

	iradix "github.com/hashicorp/go-immutable-radix"
)

type (
	// dbInfo is the internal implementation of a Transactor
	dbInfo struct {
		prefix.Sequence
		tables map[table.Name]*tableInfo
		data   *iradix.Tree
	}

	dbTransactor struct {
		*dbInfo
		transaction.Txn
	}
)

// Error messages
const (
	ErrTableAlreadyExists = "table already exists: %s"
)

// NewDatabase returns a new Transactor instance
func NewDatabase() database.Transactor {
	info := &dbInfo{
		Sequence: prefix.NewSequence(),
		tables:   map[table.Name]*tableInfo{},
		data:     iradix.New(),
	}
	return func(fn database.Func) error {
		txn := makeTransaction(info.data, func(data *iradix.Tree) {
			info.data = data
		})
		err := fn(info.transactor(txn))
		if err != nil {
			return err
		}
		txn.Commit()
		return nil
	}
}

func (info *dbInfo) transactor(txn transaction.Txn) *dbTransactor {
	return &dbTransactor{
		dbInfo: info,
		Txn:    txn,
	}
}

func (db *dbTransactor) Tables() table.Names {
	res := make(table.Names, 0, len(db.tables))
	for n := range db.tables {
		res = append(res, n)
	}
	return res
}

func (db *dbTransactor) Table(n table.Name) (table.Table, bool) {
	if tbl, ok := db.tables[n]; ok {
		return &tableTransactor{
			tableInfo: tbl,
			txn:       db.Txn,
		}, true
	}
	return nil, false
}

func (db *dbTransactor) CreateTable(
	n table.Name, cols ...column.Column,
) (table.Table, error) {
	if _, ok := db.tables[n]; ok {
		return nil, fmt.Errorf(ErrTableAlreadyExists, n)
	}

	tbl := makeTable(db, n, cols...)
	db.tables[n] = tbl
	return tbl.transactor(db.Txn), nil
}
