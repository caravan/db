package internal

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"

	radix "github.com/caravan/go-immutable-radix"
)

type (
	// txn is the internal implementation of a txn
	txn struct {
		*dbInfo
		txn *radix.Txn
	}

	// txnFor encapsulates a Prefix
	txnFor struct {
		prefix.Prefixed
		txn *radix.Txn
	}
)

func makeTransaction(db *dbInfo) *txn {
	return &txn{
		dbInfo: db,
		txn:    db.data.Txn(),
	}
}

func (t *txn) For(p prefix.Prefixed) transaction.For {
	return &txnFor{
		txn:      t.txn,
		Prefixed: p,
	}
}

func (t *txn) commit() bool {
	if data, ok := t.txn.Commit(); ok {
		t.dbInfo.data = data
		return true
	}
	return false
}

func (t *txnFor) Get(k value.Key) (transaction.Any, bool) {
	return t.txn.Get(t.Prefix().WithKey(k))
}

func (t *txnFor) Insert(
	k value.Key, v transaction.Any,
) (transaction.Any, bool) {
	return t.txn.Insert(t.Prefix().WithKey(k), v)
}

func (t *txnFor) Delete(k value.Key) (transaction.Any, bool) {
	if old, ok := t.txn.Delete(t.Prefix().WithKey(k)); ok {
		return old, ok
	}
	return nil, false
}

func (t *txnFor) Drop() bool {
	return t.txn.DeletePrefix(t.Prefix().Bytes())
}

func (t *txnFor) Ascending() transaction.Iterable {
	return MakeForwardIterable(t, t.txn)
}

func (t *txnFor) Descending() transaction.Iterable {
	return MakeReverseIterable(t, t.txn)
}
