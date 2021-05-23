package internal

import (
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"

	iradix "github.com/hashicorp/go-immutable-radix"
)

type (
	// txn is the internal implementation of a Txn
	txn struct {
		txn    *iradix.Txn
		commit committer
	}

	// committer is a function that is used to perform an internal commit
	committer func(*iradix.Tree)
)

func makeTransaction(data *iradix.Tree, commit committer) transaction.Txn {
	return &txn{
		txn:    data.Txn(),
		commit: commit,
	}
}

func (t *txn) Get(k value.Key) (transaction.Any, bool) {
	return t.txn.Get(k)
}

func (t *txn) Insert(k value.Key, v transaction.Any) (transaction.Any, bool) {
	return t.txn.Insert(k, v)
}

func (t *txn) Delete(k value.Key) (transaction.Any, bool) {
	return t.txn.Delete(k)
}

func (t *txn) DeletePrefix(p prefix.Prefix) bool {
	return t.txn.DeletePrefix(p)
}

func (t *txn) Commit() {
	t.commit(t.txn.Commit())
}
