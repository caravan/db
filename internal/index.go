package internal

import (
	"bytes"
	"fmt"

	"github.com/caravan/db/index"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"
)

type (
	// baseIndex is the base implementation of an Index
	baseIndex struct {
		name     index.Name
		prefix   prefix.Prefix
		selector relation.Selector
	}

	// uniqueIndex is the internal implementation of a unique Index
	uniqueIndex struct {
		baseIndex
	}

	// uniqueIndexMutator is an Mutator that respects unique constraints
	uniqueIndexMutator struct {
		*baseIndex
		txn transaction.Txn
	}

	// standardIndex is the internal implementation of a standard Index
	standardIndex struct {
		baseIndex
	}

	// standardIndexMutator is a standard Mutator
	standardIndexMutator struct {
		*baseIndex
		txn transaction.Txn
	}
)

// Error messages
const (
	ErrUniqueConstraintFailed = "unique constraint failed: %s"
)

func (i *baseIndex) Name() index.Name {
	return i.name
}

func (i *baseIndex) getIndexKey(r relation.Row) value.Key {
	var buf bytes.Buffer
	buf.Write(i.prefix)
	for _, cell := range i.selector(r) {
		buf.Write(cell.Bytes())
	}
	return buf.Bytes()
}

// UniqueIndex is a Type that allows only unique associations
var UniqueIndex = index.Type(
	func(p prefix.Prefix, n index.Name, s relation.Selector) index.Index {
		return &uniqueIndex{
			baseIndex: baseIndex{
				name:     n,
				selector: s,
				prefix:   p,
			},
		}
	},
)

func (i *uniqueIndex) CreateMutator(txn transaction.Txn) index.Mutator {
	return &uniqueIndexMutator{
		baseIndex: &i.baseIndex,
		txn:       txn,
	}
}

func (m *uniqueIndexMutator) Truncate() {
	m.txn.DeletePrefix(m.prefix)
}

func (m *uniqueIndexMutator) Insert(k value.Key, r relation.Row) error {
	key := m.prefix.Bytes(m.getIndexKey(r))
	if _, ok := m.txn.Get(key); ok {
		return fmt.Errorf(ErrUniqueConstraintFailed, m.name)
	}
	m.txn.Insert(key, k)
	return nil
}

func (m *uniqueIndexMutator) Delete(_ value.Key, r relation.Row) bool {
	_, ok := m.txn.Delete(m.prefix.Bytes(m.getIndexKey(r)))
	return ok
}

// StandardIndex is a Type that allows multiple associations
var StandardIndex = index.Type(
	func(p prefix.Prefix, n index.Name, s relation.Selector) index.Index {
		return &standardIndex{
			baseIndex: baseIndex{
				name:     n,
				selector: s,
				prefix:   p,
			},
		}
	},
)

func (i *standardIndex) CreateMutator(txn transaction.Txn) index.Mutator {
	return &standardIndexMutator{
		baseIndex: &i.baseIndex,
		txn:       txn,
	}
}

func (m *standardIndexMutator) Truncate() {
	m.txn.DeletePrefix(m.prefix)
}

func (m *standardIndexMutator) Insert(k value.Key, r relation.Row) error {
	key := m.prefix.Bytes(m.getIndexKey(r))
	key = append(key, k.Bytes()...)
	m.txn.Insert(key, k)
	return nil
}

func (m *standardIndexMutator) Delete(k value.Key, r relation.Row) bool {
	key := m.prefix.Bytes(m.getIndexKey(r))
	key = append(key, k.Bytes()...)
	_, ok := m.txn.Delete(key)
	return ok
}
