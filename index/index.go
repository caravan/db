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
		Mutate
		Query
	}

	Mutate interface {
		Insert(value.Key, relation.Row) error
		Delete(value.Key, relation.Row) bool
		Truncate()
	}

	Query interface {
		EQ(relation.Relation) transaction.Iterator
		NEQ(relation.Relation) transaction.Iterator
		GT(relation.Relation) transaction.Iterator
		LT(relation.Relation) transaction.Iterator
	}

	// Constructors are a set of Index Constructor
	Constructors []Constructor

	// Constructor instantiates an Index for the specified transaction.Txn
	Constructor func(txn transaction.Txn) Index

	// Type configures a Constructor for Index instances
	Type func(prefix.Prefixed, Name, relation.Selector) Constructor
)
