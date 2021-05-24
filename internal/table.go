package internal

import (
	"fmt"

	"github.com/caravan/db/column"
	"github.com/caravan/db/index"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"
)

type (
	// tableInfo is the basic internal implementation of a Table
	tableInfo struct {
		db      *dbInfo
		name    table.Name
		columns column.Columns
		offsets column.NamedOffsets
		indexes map[index.Name]index.Constructor
		prefix  prefix.Prefix
	}

	// tableTransactor is the basic implementation of a table.Transactor
	tableTransactor struct {
		*tableInfo
		txn transaction.Txn
	}

	indexerFunc func(index.Index) error
)

// Error messages
const (
	ErrIndexAlreadyExists = "index already exists in table: %s"
	ErrKeyAlreadyExists   = "key already exists in table: %s"
	ErrKeyNotFound        = "key not found in table: %s"
)

func makeTable(
	db *dbTransactor, n table.Name, cols ...column.Column,
) *tableInfo {
	return &tableInfo{
		db:      db.dbInfo,
		name:    n,
		columns: cols,
		offsets: column.MakeNamedOffsets(cols...),
		indexes: map[index.Name]index.Constructor{},
		prefix:  db.Next(),
	}
}

func (t *tableInfo) transactor(txn transaction.Txn) *tableTransactor {
	return &tableTransactor{
		tableInfo: t,
		txn:       txn,
	}
}

func (t *tableTransactor) Name() table.Name {
	return t.name
}

// Columns returns the defined Columns for this table
func (t *tableTransactor) Columns() column.Columns {
	return t.columns
}

func (t *tableTransactor) CreateIndex(
	typ index.Type, n index.Name, cols ...column.Name,
) error {
	if _, ok := t.indexes[n]; ok {
		return fmt.Errorf(ErrIndexAlreadyExists, n)
	}

	off, err := t.columnOffsets(cols)
	if err != nil {
		return err
	}

	cons := typ(t.db.Next(), n, relation.MakeOffsetSelector(off...))
	t.indexes[n] = cons
	return nil
}

// Indexes returns the defined Indexes for this table
func (t *tableTransactor) Indexes() index.Names {
	res := make(index.Names, 0, len(t.indexes))
	for n := range t.indexes {
		res = append(res, n)
	}
	return res
}

func (t *tableInfo) columnOffsets(cols column.Names) (column.Offsets, error) {
	return relation.MakeOffsets(t.columns, cols...)
}

func (t *tableTransactor) Truncate() {
	_ = t.mutateIndexes(func(i index.Index) error {
		i.Truncate()
		return nil
	})
	t.txn.DeletePrefix(t.prefix)
}

func (t *tableTransactor) Insert(k value.Key, r relation.Row) error {
	key := t.prefix.Bytes(k)
	if _, ok := t.txn.Get(key); ok {
		return fmt.Errorf(ErrKeyAlreadyExists, k)
	}
	_, _ = t.txn.Insert(key, r)
	return t.mutateIndexes(func(i index.Index) error {
		return i.Insert(k, r)
	})
}

func (t *tableTransactor) Update(
	k value.Key, r relation.Row,
) (relation.Row, error) {
	key := t.prefix.Bytes(k)
	if _, ok := t.txn.Get(key); !ok {
		return nil, fmt.Errorf(ErrKeyNotFound, k)
	}
	res, _ := t.txn.Insert(key, r)
	old := res.(relation.Row)
	err := t.mutateIndexes(func(i index.Index) error {
		i.Delete(k, old)
		return i.Insert(k, r)
	})
	if err != nil {
		return nil, err
	}
	return old, nil
}

func (t *tableTransactor) Delete(k value.Key) (relation.Row, bool) {
	res, ok := t.txn.Delete(t.prefix.Bytes(k))
	if res == nil || !ok {
		return nil, ok
	}
	row := res.(relation.Row)
	_ = t.mutateIndexes(func(i index.Index) error {
		i.Delete(k, row)
		return nil
	})
	return row, true
}

func (t *tableTransactor) Select(k value.Key) (relation.Row, bool) {
	if v, ok := t.txn.Get(t.prefix.Bytes(k)); ok {
		return v.(relation.Row), true
	}
	return nil, false
}

func (t *tableTransactor) mutateIndexes(fn indexerFunc) error {
	for _, i := range t.indexes {
		if err := fn(i(t.txn)); err != nil {
			return err
		}
	}
	return nil
}
