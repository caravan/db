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
		Insert(value.Key, relation.Row) error
		Delete(value.Key, relation.Row) bool
		Truncate()
	}

	// Constructors are a set of Index Constructor
	Constructors []Constructor

	// Constructor instantiates an Index for the specified transaction.Txn
	Constructor func(txn transaction.Txn) Index

	// Type configures a Constructor for Index instances
	Type func(prefix.Prefix, Name, relation.Selector) Constructor
)
