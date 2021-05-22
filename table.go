package db

import (
	"fmt"
	"sync"
)

type (
	// TableName identifies a Table
	TableName string

	// TableNames are a set of TableName
	TableNames []TableName

	// Table is an interface that associates a Key with a Row and provides
	// additional capabilities around this association
	Table interface {
		Name() TableName
		Columns() Columns

		MutateWith(TableMutatorFunc) error

		CreateIndex(IndexName, ...ColumnName) (Index, error)
		Indexes() IndexNames
		Index(IndexName) (Index, bool)
	}

	// TableMutatorFunc is provided in order to sequence Table mutations
	TableMutatorFunc func(TableMutator) error

	// TableMutator allows modification of the internal state of a Table
	TableMutator interface {
		Truncate()
		Insert(Key, Row) error
		Update(Key, Row) (Row, error)
		Delete(Key) (Row, bool)
		Select(Key) (Row, bool)
	}

	// table is the internal implementation of a Table
	table struct {
		sync.RWMutex
		*database
		name    TableName
		columns Columns
		offsets NamedOffsets
		indexes map[IndexName]*index
		prefix  Prefix
	}

	// tableMutator is the internal implementation of a TableMutator
	tableMutator struct {
		*table
		tx Transaction
	}
)

// Error messages
const (
	ErrIndexAlreadyExists = "index already exists in table: %s"
	ErrKeyAlreadyExists   = "key already exists in table: %s"
	ErrKeyNotFound        = "key not found in table: %s"
)

func makeTable(db *database, n TableName, cols ...Column) Table {
	return &table{
		database: db,
		name:     n,
		columns:  cols,
		offsets:  MakeNamedOffsets(cols...),
		indexes:  map[IndexName]*index{},
		prefix:   db.Next(),
	}
}

func (t *table) Name() TableName {
	return t.name
}

// Columns returns the defined Columns for this table
func (t *table) Columns() Columns {
	return t.columns
}

func (t *table) CreateIndex(n IndexName, cols ...ColumnName) (Index, error) {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.indexes[n]; ok {
		return nil, fmt.Errorf(ErrIndexAlreadyExists, n)
	}

	off, err := t.columnOffsets(cols)
	if err != nil {
		return nil, err
	}

	res := makeIndex(t, n, MakeOffsetSelector(off...))
	t.indexes[n] = res
	return res, nil
}

// Indexes returns the defined Indexes for this table
func (t *table) Indexes() IndexNames {
	t.RLock()
	defer t.RUnlock()

	res := make(IndexNames, 0, len(t.indexes))
	for n := range t.indexes {
		res = append(res, n)
	}
	return res
}

func (t *table) Index(n IndexName) (Index, bool) {
	t.RLock()
	defer t.RUnlock()
	res, ok := t.indexes[n]
	return res, ok
}

func (t *table) MutateWith(fn TableMutatorFunc) error {
	return t.mutateWith(func(tx Transaction) error {
		return fn(&tableMutator{
			table: t,
			tx:    tx,
		})
	})
}

func (t *table) mutateWith(fn TransactionalFunc) error {
	tx := t.CreateTransaction()
	if err := fn(tx); err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (t *table) columnOffsets(cols ColumnNames) (Offsets, error) {
	return MakeOffsets(t.columns, cols...)
}

func (m *tableMutator) Truncate() {
	m.mutateIndexes(func(i IndexMutator) {
		i.Truncate()
	})
	m.tx.DeletePrefix(m.prefix)
}

func (m *tableMutator) Insert(k Key, r Row) error {
	key := m.prefix.Bytes(k)
	if _, ok := m.tx.Get(key); ok {
		return fmt.Errorf(ErrKeyAlreadyExists, k)
	}
	_, _ = m.tx.Insert(key, r)
	m.mutateIndexes(func(i IndexMutator) {
		i.Insert(k, r)
	})
	return nil
}

func (m *tableMutator) Update(k Key, r Row) (Row, error) {
	key := m.prefix.Bytes(k)
	if _, ok := m.tx.Get(key); !ok {
		return nil, fmt.Errorf(ErrKeyNotFound, k)
	}
	res, _ := m.tx.Insert(key, r)
	old := res.(Row)
	m.mutateIndexes(func(i IndexMutator) {
		i.Delete(k, old)
		i.Insert(k, r)
	})
	return old, nil
}

func (m *tableMutator) Delete(k Key) (Row, bool) {
	res, ok := m.tx.Delete(m.prefix.Bytes(k))
	if res == nil || !ok {
		return nil, ok
	}
	row := res.(Row)
	m.mutateIndexes(func(i IndexMutator) {
		i.Delete(k, row)
	})
	return row, true
}

func (m *tableMutator) Select(k Key) (Row, bool) {
	if v, ok := m.tx.Get(m.prefix.Bytes(k)); ok {
		return v.(Row), true
	}
	return nil, false
}

func (m *tableMutator) mutateIndexes(fn func(IndexMutator)) {
	for _, i := range m.indexes {
		m := &indexMutator{
			tx:    m.tx,
			index: i,
		}
		fn(m)
	}
}
