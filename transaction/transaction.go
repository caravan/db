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
		DeletePrefix(prefix.Prefix) bool
		Get(value.Key) (Any, bool)
		ForEach(prefix.Prefix, Reporter) error
	}

	// Reporter is called by ForEach
	Reporter func(value.Key, Any) error

	// WriterFunc is provided in order to sequence transactional writes
	WriterFunc func(Txn) error
)
