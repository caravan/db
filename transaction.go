package db

import iradix "github.com/hashicorp/go-immutable-radix"

type (
	// Any is a stand-in for Go's empty interface
	Any interface{}

	// Transaction manages the types of events that can be performed at
	// the most basic level of the storage system
	Transaction interface {
		Get(Key) (Any, bool)
		Insert(Key, Any) (Any, bool)
		Delete(Key) (Any, bool)
		DeletePrefix(Prefix) bool
		Commit()
	}

	// TransactionalFunc is provided in order to sequence Transaction events
	TransactionalFunc func(Transaction) error

	// transaction is the internal implementation of a Transaction
	transaction struct {
		tx     *iradix.Txn
		commit committer
	}

	// committer is a function that is used to perform an internal commit
	committer func(*iradix.Tree)
)

func makeTransaction(data *iradix.Tree, commit committer) Transaction {
	return &transaction{
		tx:     data.Txn(),
		commit: commit,
	}
}

func (t *transaction) Get(k Key) (Any, bool) {
	return t.tx.Get(k)
}

func (t *transaction) Insert(k Key, v Any) (Any, bool) {
	return t.tx.Insert(k, v)
}

func (t *transaction) Delete(k Key) (Any, bool) {
	return t.tx.Delete(k)
}

func (t *transaction) DeletePrefix(p Prefix) bool {
	return t.tx.DeletePrefix(p)
}

func (t *transaction) Commit() {
	t.commit(t.tx.Commit())
}
