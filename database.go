package db

import (
	"fmt"
	"sync"

	iradix "github.com/hashicorp/go-immutable-radix"
)

type (
	// Database is an interface that manages a set of Tables and other
	// data management structures
	Database interface {
		CreateTransaction() Transaction

		CreateTable(TableName, ...Column) (Table, error)
		Tables() TableNames
		Table(TableName) (Table, bool)
	}

	// database is the internal implementation of a Database
	database struct {
		sync.RWMutex
		Sequence
		tables map[TableName]Table
		data   *iradix.Tree
	}
)

// Error messages
const (
	ErrTableAlreadyExists = "table already exists: %s"
)

// NewDatabase returns a new Database instance
func NewDatabase() Database {
	return &database{
		Sequence: NewSequence(),
		tables:   map[TableName]Table{},
		data:     iradix.New(),
	}
}

func (d *database) CreateTransaction() Transaction {
	return makeTransaction(d.data, func(data *iradix.Tree) {
		d.Lock()
		defer d.Unlock()
		d.data = data
	})
}

func (d *database) CreateTable(n TableName, cols ...Column) (Table, error) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.tables[n]; ok {
		return nil, fmt.Errorf(ErrTableAlreadyExists, n)
	}

	res := makeTable(d, n, cols...)
	d.tables[n] = res
	return res, nil
}

func (d *database) Tables() TableNames {
	d.RLock()
	defer d.RUnlock()

	res := make(TableNames, 0, len(d.tables))
	for n := range d.tables {
		res = append(res, n)
	}
	return res
}

func (d *database) Table(n TableName) (Table, bool) {
	d.RLock()
	defer d.RUnlock()
	res, ok := d.tables[n]
	return res, ok
}
