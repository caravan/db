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
		indexes prefix.Prefix
		rows    prefix.Prefix
	}

	// tableTxr is the basic implementation of a table.Transactor
	tableTxr struct {
		*tableInfo
		*dbTxr
	}

	indexerFunc func(index.Index) error
)

// Error messages
const (
	ErrIndexAlreadyExists = "index already exists in table: %s"
	ErrKeyAlreadyExists   = "key already exists in table: %s"
	ErrKeyNotFound        = "key not found in table: %s"
)

func makeTable(db *dbTxr, n table.Name, cols ...column.Column) *tableInfo {
	return &tableInfo{
		db:      db.dbInfo,
		name:    n,
		columns: cols,
		offsets: column.MakeNamedOffsets(cols...),
		indexes: db.nextPrefix(),
		rows:    db.nextPrefix(),
	}
}

func (t *tableInfo) transactor(db *dbTxr) *tableTxr {
	return &tableTxr{
		tableInfo: t,
		dbTxr:     db,
	}
}

func (t *tableInfo) indexKey(n index.Name) value.Key {
	return t.indexes.WithKey([]byte(n))
}

func (t *tableInfo) rowKey(k value.Key) value.Key {
	return t.rows.WithKey(k)
}

func (t *tableTxr) Name() table.Name {
	return t.name
}

// Columns returns the defined Columns for this table
func (t *tableTxr) Columns() column.Columns {
	return t.columns
}

func (t *tableTxr) CreateIndex(
	typ index.Type, n index.Name, cols ...column.Name,
) error {
	key := t.indexKey(n)
	if _, ok := t.Txn.Get(key); ok {
		return fmt.Errorf(ErrIndexAlreadyExists, n)
	}

	off, err := t.columnOffsets(cols)
	if err != nil {
		return err
	}

	pfx := t.nextPrefix()
	cons := typ(pfx, n, relation.MakeOffsetSelector(off...))
	t.Txn.Insert(key, cons)
	return nil
}

// Indexes returns the defined Indexes for this table
func (t *tableTxr) Indexes() index.Names {
	var res index.Names
	_ = t.Txn.ForEach(t.indexes, func(k value.Key, v transaction.Any) error {
		name := index.Name(k)
		res = append(res, name)
		return nil
	})
	return res
}

func (t *tableInfo) columnOffsets(cols column.Names) (column.Offsets, error) {
	return relation.MakeOffsets(t.columns, cols...)
}

func (t *tableTxr) Truncate() {
	_ = t.mutateIndexes(func(i index.Index) error {
		i.Truncate()
		return nil
	})
	t.Txn.DeletePrefix(t.rows)
}

func (t *tableTxr) Insert(k value.Key, r relation.Row) error {
	key := t.rowKey(k)
	if _, ok := t.Txn.Get(key); ok {
		return fmt.Errorf(ErrKeyAlreadyExists, k)
	}
	_, _ = t.Txn.Insert(key, r)
	return t.mutateIndexes(func(i index.Index) error {
		return i.Insert(k, r)
	})
}

func (t *tableTxr) Update(k value.Key, r relation.Row) (relation.Row, error) {
	key := t.rowKey(k)
	if _, ok := t.Txn.Get(key); !ok {
		return nil, fmt.Errorf(ErrKeyNotFound, k)
	}
	res, _ := t.Txn.Insert(key, r)
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

func (t *tableTxr) Delete(k value.Key) (relation.Row, bool) {
	res, ok := t.Txn.Delete(t.rowKey(k))
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

func (t *tableTxr) Select(k value.Key) (relation.Row, bool) {
	if v, ok := t.Txn.Get(t.rowKey(k)); ok {
		return v.(relation.Row), true
	}
	return nil, false
}

func (t *tableTxr) mutateIndexes(fn indexerFunc) error {
	return t.Txn.ForEach(t.indexes,
		func(k value.Key, v transaction.Any) error {
			cons := v.(index.Constructor)
			if err := fn(cons(t.Txn)); err != nil {
				return err
			}
			return nil
		},
	)
}
