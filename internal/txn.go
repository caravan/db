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

func (t *txn) Get(k value.Key) (transaction.Any, bool) {
	return t.txn.Get(k)
}

func (t *txn) Insert(k value.Key, v transaction.Any) (transaction.Any, bool) {
	return t.txn.Insert(k, v)
}

func (t *txn) Delete(k value.Key) (transaction.Any, bool) {
	if old, ok := t.txn.Delete(k); ok {
		return old, ok
	}
	return nil, false
}

func (t *txn) DeletePrefix(p prefix.Prefix) bool {
	return t.txn.DeletePrefix(p.Bytes())
}

func (t *txn) ForEach(p prefix.Prefix, fn transaction.Reporter) error {
	pfx := append(p.Bytes(), 0)
	start := len(pfx)
	iter := t.txn.Root().Iterator()
	iter.SeekPrefix(pfx)
	for k, v, ok := iter.Next(); ok; k, v, ok = iter.Next() {
		if err := fn(k[start:], v); err != nil {
			return err
		}
	}
	return nil
}

func (t *txn) commit() bool {
	if data, ok := t.txn.Commit(); ok {
		t.dbInfo.data = data
		return true
	}
	return false
}
