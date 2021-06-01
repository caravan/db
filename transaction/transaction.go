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
		Insert(prefix.Prefixed, value.Key, Any) (Any, bool)
		Delete(prefix.Prefixed, value.Key) (Any, bool)
		Get(prefix.Prefixed, value.Key) (Any, bool)

		DeletePrefix(prefix.Prefixed) bool
		Ascending(prefix.Prefixed) Iterable
		Descending(prefix.Prefixed) Iterable
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
