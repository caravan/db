package internal

import (
	"fmt"

	"github.com/caravan/db/column"
	"github.com/caravan/db/index"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction/iterate"
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
	idx := t.txn.For(t.indexes)
	key := value.Key(n)
	if _, ok := idx.Get(key); ok {
		return fmt.Errorf(ErrIndexAlreadyExists, n)
	}

	off, err := t.columnOffsets(cols)
	if err != nil {
		return err
	}

	pfx := t.nextPrefix()
	cons := typ(pfx, n, relation.MakeOffsetSelector(off...))
	idx.Insert(key, cons)
	return nil
}

// Indexes returns the defined Indexes for this table
func (t *tableTxr) Indexes() index.Names {
	var res index.Names
	_ = iterate.ForEach(t.txn.For(t.indexes).Ascending().All(),
		func(k value.Key, v any) error {
			name := index.Name(k)
			res = append(res, name)
			return nil
		},
	)
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
	t.txn.For(t.rows).Drop()
}

func (t *tableTxr) Insert(k value.Key, r relation.Row) error {
	rows := t.txn.For(t.rows)
	if _, ok := rows.Get(k); ok {
		return fmt.Errorf(ErrKeyAlreadyExists, k)
	}
	_, _ = rows.Insert(k, r)
	return t.mutateIndexes(func(i index.Index) error {
		return i.Insert(k, r)
	})
}

func (t *tableTxr) Update(k value.Key, r relation.Row) (relation.Row, error) {
	rows := t.txn.For(t.rows)
	if _, ok := rows.Get(k); !ok {
		return nil, fmt.Errorf(ErrKeyNotFound, k)
	}
	res, _ := rows.Insert(k, r)
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
	res, ok := t.txn.For(t.rows).Delete(k)
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
	if v, ok := t.txn.For(t.rows).Get(k); ok {
		return v.(relation.Row), true
	}
	return nil, false
}

func (t *tableTxr) mutateIndexes(fn indexerFunc) error {
	return iterate.ForEach(t.txn.For(t.indexes).Ascending().All(),
		func(k value.Key, v any) error {
			cons := v.(index.Constructor)
			if err := fn(cons(t.txn)); err != nil {
				return err
			}
			return nil
		},
	)
}
