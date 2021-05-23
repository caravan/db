package transaction

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/value"
)

type (
	// Any is a stand-in for Go's empty interface
	Any interface{}

	// Txn manages the types of events that can be performed at
	// the most basic level of the storage system
	Txn interface {
		Get(value.Key) (Any, bool)
		Insert(value.Key, Any) (Any, bool)
		Delete(value.Key) (Any, bool)
		DeletePrefix(prefix.Prefix) bool
		Commit()
	}

	// TransactionalFunc is provided in order to sequence Txn events
	TransactionalFunc func(Txn) error
)
