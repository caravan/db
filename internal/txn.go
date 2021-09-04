package internal

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"

	radix "github.com/caravan/go-immutable-radix"
)

type (
	// txn is the internal implementation of a transaction.Txn
	txn struct {
		*dbInfo
		*radix.Txn
	}

	// txnFor encapsulates a prefix.Prefixed
	txnFor struct {
		*txn
		prefix.Prefixed
	}
)

func makeTransaction(db *dbInfo) *txn {
	return &txn{
		dbInfo: db,
		Txn:    db.data.Txn(),
	}
}

func (t *txn) For(p prefix.Prefixed) transaction.For {
	return &txnFor{
		txn:      t,
		Prefixed: p,
	}
}

func (t *txn) commit() bool {
	if data, ok := t.Txn.Commit(); ok {
		t.dbInfo.data = data
		return true
	}
	return false
}

func (t *txnFor) Get(k value.Key) (transaction.Any, bool) {
	key := t.Prefix().WithKey(k)
	return t.Txn.Get(key)
}

func (t *txnFor) Insert(
	k value.Key, v transaction.Any,
) (transaction.Any, bool) {
	key := t.Prefix().WithKey(k)
	return t.Txn.Insert(key, v)
}

func (t *txnFor) Delete(k value.Key) (transaction.Any, bool) {
	key := t.Prefix().WithKey(k)
	if old, ok := t.Txn.Delete(key); ok {
		return old, ok
	}
	return nil, false
}

func (t *txnFor) Drop() bool {
	pfx := t.Prefix().Bytes()
	return t.Txn.DeletePrefix(pfx)
}

func (t *txnFor) Query() transaction.Query {
	return t
}

func (t *txnFor) Ascending() transaction.Iterable {
	return ForwardIterable(t, t.Txn)
}

func (t *txnFor) Descending() transaction.Iterable {
	return ReverseIterable(t, t.Txn)
}
