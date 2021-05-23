package internal

import (
	"fmt"
	"sync"

	"github.com/caravan/db/column"
	"github.com/caravan/db/database"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"

	iradix "github.com/hashicorp/go-immutable-radix"
)

// db is the internal implementation of a Database
type db struct {
	sync.RWMutex
	prefix.Sequence
	tables map[table.Name]table.Table
	data   *iradix.Tree
}

// Error messages
const (
	ErrTableAlreadyExists = "table already exists: %s"
)

// NewDatabase returns a new Database instance
func NewDatabase() database.Database {
	return &db{
		Sequence: prefix.NewSequence(),
		tables:   map[table.Name]table.Table{},
		data:     iradix.New(),
	}
}

func (db *db) CreateTransaction() transaction.Txn {
	return makeTransaction(db.data, func(data *iradix.Tree) {
		db.Lock()
		defer db.Unlock()
		db.data = data
	})
}

func (db *db) CreateTable(
	n table.Name, cols ...column.Column,
) (table.Table, error) {
	db.Lock()
	defer db.Unlock()

	if _, ok := db.tables[n]; ok {
		return nil, fmt.Errorf(ErrTableAlreadyExists, n)
	}

	res := makeTable(db, n, cols...)
	db.tables[n] = res
	return res, nil
}

func (db *db) Tables() table.Names {
	db.RLock()
	defer db.RUnlock()

	res := make(table.Names, 0, len(db.tables))
	for n := range db.tables {
		res = append(res, n)
	}
	return res
}

func (db *db) Table(n table.Name) (table.Table, bool) {
	db.RLock()
	defer db.RUnlock()
	res, ok := db.tables[n]
	return res, ok
}
