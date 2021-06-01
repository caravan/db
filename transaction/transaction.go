package transaction

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/value"
)

type (
	// Any is a stand-in for Go's empty interface
	Any interface{}

	// Txn manages the types of events that can be performed at the
	// most basic level of the storage system
	Txn interface {
		For(prefix.Prefixed) For
	}

	// For exposes events that are bound to a specific Prefixed
	For interface {
		Insert(value.Key, Any) (Any, bool)
		Delete(value.Key) (Any, bool)
		Get(value.Key) (Any, bool)

		Drop() bool
		Ascending() Iterable
		Descending() Iterable
	}

	// Iterable can be used to generate an Iterator
	Iterable interface {
		All() Iterator
		From(value.Key) Iterator
	}

	// Iterator is stateless iteration interface
	Iterator func() (value.Key, Any, Iterator, bool)

	// WriterFunc is provided in order to sequence transactional writes
	WriterFunc func(Txn) error
)
