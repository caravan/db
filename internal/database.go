package internal

import (
	"fmt"
	"sync"

	"github.com/caravan/db/column"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"

	iradix "github.com/hashicorp/go-immutable-radix"
)

// database is the internal implementation of a Database
type database struct {
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
func NewDatabase() *database {
	return &database{
		Sequence: prefix.NewSequence(),
		tables:   map[table.Name]table.Table{},
		data:     iradix.New(),
	}
}

func (d *database) CreateTransaction() transaction.Txn {
	return makeTransaction(d.data, func(data *iradix.Tree) {
		d.Lock()
		defer d.Unlock()
		d.data = data
	})
}

func (d *database) CreateTable(
	n table.Name, cols ...column.Column,
) (table.Table, error) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.tables[n]; ok {
		return nil, fmt.Errorf(ErrTableAlreadyExists, n)
	}

	res := makeTable(d, n, cols...)
	d.tables[n] = res
	return res, nil
}

func (d *database) Tables() table.Names {
	d.RLock()
	defer d.RUnlock()

	res := make(table.Names, 0, len(d.tables))
	for n := range d.tables {
		res = append(res, n)
	}
	return res
}

func (d *database) Table(n table.Name) (table.Table, bool) {
	d.RLock()
	defer d.RUnlock()
	res, ok := d.tables[n]
	return res, ok
}
