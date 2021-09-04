package internal

import (
	"fmt"

	"github.com/caravan/db/index"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/transaction/iterate"
	"github.com/caravan/db/value"
)

type (
	// indexInfo is the base implementation of an Index
	indexInfo struct {
		prefix.Prefixed
		name     index.Name
		selector relation.Selector
	}

	baseIndex struct {
		*indexInfo
		txn transaction.Txn
	}

	uniqueIndex   struct{ baseIndex }
	standardIndex struct{ baseIndex }
)

// Error messages
const (
	ErrUniqueConstraintFailed = "unique constraint failed: %s"
)

func makeIndexInfo(
	p prefix.Prefixed, n index.Name, s relation.Selector,
) *indexInfo {
	return &indexInfo{
		name:     n,
		selector: s,
		Prefixed: p,
	}
}

func (i *indexInfo) keysForValues(v ...value.Value) []value.Key {
	var keys []value.Key
	for _, cell := range i.selector(v) {
		keys = append(keys, cell.Bytes())
	}
	return keys
}

func (i *indexInfo) keyForValues(v ...value.Value) value.Key {
	keys := i.keysForValues(v...)
	return value.JoinKeys(keys...)
}

func makeBaseIndex(info *indexInfo, txn transaction.Txn) baseIndex {
	return baseIndex{
		indexInfo: info,
		txn:       txn,
	}
}

func (i *baseIndex) Truncate() {
	i.txn.For(i).Drop()
}

func (i *baseIndex) EQ(r relation.Relation) transaction.Iterator {
	pfx := i.keyForValues(r...)
	iter := i.txn.For(i).Ascending().From(pfx)
	return iterate.While(iter, func(k value.Key, _ transaction.Any) bool {
		return k.Compare(pfx) == value.EqualTo
	})
}

func (i *baseIndex) NEQ(r relation.Relation) transaction.Iterator {
	pfx := i.keyForValues(r...)
	iter := i.txn.For(i).Ascending().All()
	return iterate.While(iter, func(k value.Key, _ transaction.Any) bool {
		return k.Compare(pfx) != value.EqualTo
	})
}

func (i *baseIndex) LT(r relation.Relation) transaction.Iterator {
	pfx := i.keyForValues(r...)
	iter := i.txn.For(i).Ascending().All()
	return iterate.While(iter, func(k value.Key, _ transaction.Any) bool {
		return k.Compare(pfx) == value.LessThan
	})
}

func (i *baseIndex) GT(r relation.Relation) transaction.Iterator {
	pfx := i.keyForValues(r...)
	iter := i.txn.For(i).Ascending().All()
	return iterate.While(iter, func(k value.Key, _ transaction.Any) bool {
		return k.Compare(pfx) == value.GreaterThan
	})
}

// UniqueIndex is an index.Type that allows only unique associations
var UniqueIndex = index.Type(
	func(p prefix.Prefixed, n index.Name, s relation.Selector) index.Constructor {
		info := makeIndexInfo(p, n, s)
		return func(txn transaction.Txn) index.Index {
			return &uniqueIndex{
				baseIndex: makeBaseIndex(info, txn),
			}
		}
	},
)

func (w *uniqueIndex) Insert(k value.Key, r relation.Row) error {
	idx := w.txn.For(w)
	key := w.keyForValues(r...)
	if _, ok := idx.Get(key); ok {
		return fmt.Errorf(ErrUniqueConstraintFailed, w.name)
	}
	idx.Insert(key, k)
	return nil
}

func (w *uniqueIndex) Delete(_ value.Key, r relation.Row) bool {
	key := w.keyForValues(r...)
	_, ok := w.txn.For(w).Delete(key)
	return ok
}

// StandardIndex is an index.Type that allows multiple associations
var StandardIndex = index.Type(
	func(p prefix.Prefixed, n index.Name, s relation.Selector) index.Constructor {
		info := makeIndexInfo(p, n, s)
		return func(txn transaction.Txn) index.Index {
			return &standardIndex{
				baseIndex: makeBaseIndex(info, txn),
			}
		}
	},
)

func (i *standardIndex) Insert(k value.Key, r relation.Row) error {
	keys := append(i.keysForValues(r...), k)
	key := value.JoinKeys(keys...)
	i.txn.For(i).Insert(key, k)
	return nil
}

func (i *standardIndex) Delete(k value.Key, r relation.Row) bool {
	keys := append(i.keysForValues(r...), k)
	key := value.JoinKeys(keys...)
	_, ok := i.txn.For(i).Delete(key)
	return ok
}
