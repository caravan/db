package internal

import (
	"fmt"
	"sync"

	"github.com/caravan/db/column"
	"github.com/caravan/db/index"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"
)

type (
	// basicTable is the basic internal implementation of a Table
	basicTable struct {
		sync.RWMutex
		*database
		name    table.Name
		columns column.Columns
		offsets column.NamedOffsets
		indexes map[index.Name]index.Index
		prefix  prefix.Prefix
	}

	// basicTableMutator is the basic internal implementation of a Mutator
	basicTableMutator struct {
		*basicTable
		txn transaction.Txn
	}
)

// Error messages
const (
	ErrIndexAlreadyExists = "index already exists in table: %s"
	ErrKeyAlreadyExists   = "key already exists in table: %s"
	ErrKeyNotFound        = "key not found in table: %s"
)

func makeTable(
	db *database, n table.Name, cols ...column.Column,
) *basicTable {
	return &basicTable{
		database: db,
		name:     n,
		columns:  cols,
		offsets:  column.MakeNamedOffsets(cols...),
		indexes:  map[index.Name]index.Index{},
		prefix:   db.Next(),
	}
}

func (t *basicTable) Name() table.Name {
	return t.name
}

// Columns returns the defined Columns for this table
func (t *basicTable) Columns() column.Columns {
	return t.columns
}

func (t *basicTable) CreateIndex(
	makeIndex index.Type, n index.Name, cols ...column.Name,
) (index.Index, error) {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.indexes[n]; ok {
		return nil, fmt.Errorf(ErrIndexAlreadyExists, n)
	}

	off, err := t.columnOffsets(cols)
	if err != nil {
		return nil, err
	}

	res := makeIndex(t.Next(), n, relation.MakeOffsetSelector(off...))
	t.indexes[n] = res
	return res, nil
}

// Indexes returns the defined Indexes for this table
func (t *basicTable) Indexes() index.Names {
	t.RLock()
	defer t.RUnlock()

	res := make(index.Names, 0, len(t.indexes))
	for n := range t.indexes {
		res = append(res, n)
	}
	return res
}

func (t *basicTable) Index(n index.Name) (index.Index, bool) {
	t.RLock()
	defer t.RUnlock()
	res, ok := t.indexes[n]
	return res, ok
}

func (t *basicTable) MutateWith(fn table.MutatorFunc) error {
	return t.mutateWith(func(txn transaction.Txn) error {
		return fn(&basicTableMutator{
			basicTable: t,
			txn:        txn,
		})
	})
}

func (t *basicTable) mutateWith(fn transaction.TransactionalFunc) error {
	txn := t.CreateTransaction()
	if err := fn(txn); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (t *basicTable) columnOffsets(cols column.Names) (column.Offsets, error) {
	return relation.MakeOffsets(t.columns, cols...)
}

func (m *basicTableMutator) Truncate() {
	_ = m.mutateIndexes(func(i index.Mutator) error {
		i.Truncate()
		return nil
	})
	m.txn.DeletePrefix(m.prefix)
}

func (m *basicTableMutator) Insert(k value.Key, r relation.Row) error {
	key := m.prefix.Bytes(k)
	if _, ok := m.txn.Get(key); ok {
		return fmt.Errorf(ErrKeyAlreadyExists, k)
	}
	_, _ = m.txn.Insert(key, r)
	return m.mutateIndexes(func(i index.Mutator) error {
		return i.Insert(k, r)
	})
}

func (m *basicTableMutator) Update(
	k value.Key, r relation.Row,
) (relation.Row, error) {
	key := m.prefix.Bytes(k)
	if _, ok := m.txn.Get(key); !ok {
		return nil, fmt.Errorf(ErrKeyNotFound, k)
	}
	res, _ := m.txn.Insert(key, r)
	old := res.(relation.Row)
	err := m.mutateIndexes(func(i index.Mutator) error {
		i.Delete(k, old)
		return i.Insert(k, r)
	})
	if err != nil {
		return nil, err
	}
	return old, nil
}

func (m *basicTableMutator) Delete(k value.Key) (relation.Row, bool) {
	res, ok := m.txn.Delete(m.prefix.Bytes(k))
	if res == nil || !ok {
		return nil, ok
	}
	row := res.(relation.Row)
	_ = m.mutateIndexes(func(i index.Mutator) error {
		i.Delete(k, row)
		return nil
	})
	return row, true
}

func (m *basicTableMutator) Select(k value.Key) (relation.Row, bool) {
	if v, ok := m.txn.Get(m.prefix.Bytes(k)); ok {
		return v.(relation.Row), true
	}
	return nil, false
}

func (m *basicTableMutator) mutateIndexes(fn index.MutatorFunc) error {
	for _, i := range m.indexes {
		m := i.CreateMutator(m.txn)
		if err := fn(m); err != nil {
			return err
		}
	}
	return nil
}
