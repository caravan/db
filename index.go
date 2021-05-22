package db

import (
	"bytes"
	"fmt"
)

type (
	// IndexName identifies an Index
	IndexName string

	// IndexNames is a set of IndexName
	IndexNames []IndexName

	// Index describes a lookup structure associated with a Table
	Index interface {
		Name() IndexName
		CreateMutator(Transaction) IndexMutator
	}

	// IndexMutatorFunc is provided in order to sequence Index mutations
	IndexMutatorFunc func(IndexMutator) error

	// IndexMutator allows modification of the internal state of an Index
	IndexMutator interface {
		Truncate()
		Insert(Key, Row) error
		Delete(Key, Row) bool
	}

	// Indexes are a set of Index
	Indexes []Index

	// IndexType is used to construct new Index instances
	IndexType func(Prefix, IndexName, Selector) Index

	// index is the internal implementation of an Index
	index struct {
		name     IndexName
		selector Selector
		prefix   Prefix
		unique   bool
	}

	uniqueIndex struct {
		index
	}

	// uniqueIndexMutator is an IndexMutator that respects unique constraints
	uniqueIndexMutator struct {
		*index
		tx Transaction
	}
)

// Error messages
const (
	ErrUniqueConstraintFailed = "unique constraint failed: %s"
)

// UniqueIndex is an IndexType that allows only unique associations
var UniqueIndex = IndexType(func(p Prefix, n IndexName, s Selector) Index {
	return &uniqueIndex{
		index: index{
			name:     n,
			selector: s,
			prefix:   p,
		},
	}
})

func (i *index) Name() IndexName {
	return i.name
}

func (i *uniqueIndex) CreateMutator(tx Transaction) IndexMutator {
	return &uniqueIndexMutator{
		index: &i.index,
		tx:    tx,
	}
}

func (i *index) getIndexKey(r Row) Key {
	var buf bytes.Buffer
	buf.Write(i.prefix)
	for _, cell := range i.selector(r) {
		buf.Write(cell.Bytes())
	}
	return buf.Bytes()
}

func (m *uniqueIndexMutator) Truncate() {
	m.tx.DeletePrefix(m.prefix)
}

func (m *uniqueIndexMutator) Insert(k Key, r Row) error {
	key := m.prefix.Bytes(m.getIndexKey(r))
	if _, ok := m.tx.Get(key); ok {
		return fmt.Errorf(ErrUniqueConstraintFailed, m.name)
	}
	m.tx.Insert(key, k)
	return nil
}

func (m *uniqueIndexMutator) Delete(_ Key, r Row) bool {
	_, ok := m.tx.Delete(m.prefix.Bytes(m.getIndexKey(r)))
	return ok
}
