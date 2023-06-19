package transaction

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/value"
)

type (
	// Txn manages the types of actions that can be performed at the
	// most basic level of the storage system
	Txn interface {
		For(prefix.Prefixed) For
	}

	// For exposes actions that are bound to a specific Prefixed
	For interface {
		Mutate
		Query
	}

	// Mutate allows changed to data of a Prefixed For
	Mutate interface {
		Insert(value.Key, any) (any, bool)
		Delete(value.Key) (any, bool)
		Drop() bool
	}

	// Query allows retrieval of data from a Prefixed For
	Query interface {
		Get(value.Key) (any, bool)
		Ascending() Iterable
		Descending() Iterable
	}

	// Iterable can be used to generate an Iterator
	Iterable interface {
		All() Iterator
		From(value.Key) Iterator
	}

	// Iterator is stateless iteration interface
	Iterator func() (value.Key, any, Iterator, bool)

	// WriterFunc is provided in order to sequence transactional writes
	WriterFunc func(Txn) error
)
