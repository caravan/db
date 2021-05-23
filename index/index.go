package index

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"
)

type (
	// Name identifies an Index
	Name string

	// Names is a set of Name
	Names []Name

	// Index describes a lookup structure associated with a Table
	Index interface {
		Name() Name
		CreateMutator(transaction.Txn) Mutator
	}

	// MutatorFunc is provided in order to sequence Index mutations
	MutatorFunc func(Mutator) error

	// Mutator allows modification of the internal state of an Index
	Mutator interface {
		Truncate()
		Insert(value.Key, relation.Row) error
		Delete(value.Key, relation.Row) bool
	}

	// Indexes are a set of Index
	Indexes []Index

	// Type is used to construct new Index instances
	Type func(prefix.Prefix, Name, relation.Selector) Index
)
