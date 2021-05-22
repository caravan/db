package db

import "bytes"

type (
	// IndexName identifies an Index
	IndexName string

	// IndexNames is a set of IndexName
	IndexNames []IndexName

	// Index describes a lookup structure associated with a Table
	Index interface {
		Name() IndexName
	}

	// IndexMutator allows modification of the internal state of an Index
	IndexMutator interface {
		Truncate()
		Insert(Key, Row)
		Delete(Key, Row)
	}

	// Indexes are a set of Index
	Indexes []Index

	// index is the internal implementation of an Index
	index struct {
		*table
		name     IndexName
		selector Selector
		prefix   Prefix
	}

	// indexMutator is the internal implementation of an IndexMutator
	indexMutator struct {
		*index
		tx Transaction
	}
)

func makeIndex(t *table, n IndexName, s Selector) *index {
	return &index{
		table:    t,
		name:     n,
		selector: s,
		prefix:   t.Next(),
	}
}

func (i *index) Name() IndexName {
	return i.name
}

func (i *index) getIndexKey(r Row) Key {
	var buf bytes.Buffer
	buf.Write(i.prefix)
	for _, cell := range i.selector(r) {
		buf.Write(cell.Bytes())
	}
	return buf.Bytes()
}

func (m *indexMutator) Truncate() {
	m.tx.DeletePrefix(m.prefix)
}

func (m *indexMutator) Insert(key Key, r Row) {
	m.tx.Insert(m.prefix.Bytes(m.getIndexKey(r)), key)
}

func (m *indexMutator) Delete(_ Key, r Row) {
	m.tx.Delete(m.prefix.Bytes(m.getIndexKey(r)))
}
