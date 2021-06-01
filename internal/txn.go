package internal

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"

	radix "github.com/caravan/go-immutable-radix"
)

// txn is the internal implementation of a txn
type txn struct {
	*dbInfo
	txn *radix.Txn
}

func makeTransaction(db *dbInfo) *txn {
	return &txn{
		dbInfo: db,
		txn:    db.data.Txn(),
	}
}

func (t *txn) Get(p prefix.Prefixed, k value.Key) (transaction.Any, bool) {
	return t.txn.Get(p.Prefix().WithKey(k))
}

func (t *txn) Insert(
	p prefix.Prefixed, k value.Key, v transaction.Any,
) (transaction.Any, bool) {
	return t.txn.Insert(p.Prefix().WithKey(k), v)
}

func (t *txn) Delete(p prefix.Prefixed, k value.Key) (transaction.Any, bool) {
	if old, ok := t.txn.Delete(p.Prefix().WithKey(k)); ok {
		return old, ok
	}
	return nil, false
}

func (t *txn) DeletePrefix(p prefix.Prefixed) bool {
	return t.txn.DeletePrefix(p.Prefix().Bytes())
}

func (t *txn) Ascending(p prefix.Prefixed) transaction.Iterable {
	return MakeForwardIterable(p, t.txn)
}

func (t *txn) Descending(p prefix.Prefixed) transaction.Iterable {
	return MakeReverseIterable(p, t.txn)
}

func (t *txn) commit() bool {
	if data, ok := t.txn.Commit(); ok {
		t.dbInfo.data = data
		return true
	}
	return false
}
