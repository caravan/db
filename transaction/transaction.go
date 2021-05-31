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
		Insert(value.Key, Any) (Any, bool)
		Delete(value.Key) (Any, bool)
		Get(value.Key) (Any, bool)

		DeletePrefix(prefix.Prefix) bool
		Ascending(prefix.Prefix) Iterable
		Descending(prefix.Prefix) Iterable
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
