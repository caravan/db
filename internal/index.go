package internal

import (
	"fmt"

	"github.com/caravan/db/index"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"
)

type (
	// indexInfo is the base implementation of an Index
	indexInfo struct {
		name     index.Name
		prefix   prefix.Prefix
		selector relation.Selector
	}

	// uniqueIndex is the internal implementation of a unique Index
	uniqueIndex struct {
		*indexInfo
		txn transaction.Txn
	}

	// standardIndex is the internal implementation of a standard Index
	standardIndex struct {
		*indexInfo
		txn transaction.Txn
	}
)

// Error messages
const (
	ErrUniqueConstraintFailed = "unique constraint failed: %s"
)

func (i *indexInfo) keysForRow(r relation.Row) []value.Key {
	var keys []value.Key
	for _, cell := range i.selector(r) {
		keys = append(keys, cell.Bytes())
	}
	return keys
}

func makeIndexInfo(
	p prefix.Prefix, n index.Name, s relation.Selector,
) *indexInfo {
	return &indexInfo{
		name:     n,
		selector: s,
		prefix:   p,
	}
}

// UniqueIndex is an index.Type that allows only unique associations
var UniqueIndex = index.Type(
	func(p prefix.Prefix, n index.Name, s relation.Selector) index.Constructor {
		info := makeIndexInfo(p, n, s)
		return func(txn transaction.Txn) index.Index {
			return &uniqueIndex{
				indexInfo: info,
				txn:       txn,
			}
		}
	},
)

func (w *uniqueIndex) Insert(k value.Key, r relation.Row) error {
	key := w.prefix.WithKeys(w.keysForRow(r)...)
	if _, ok := w.txn.Get(key); ok {
		return fmt.Errorf(ErrUniqueConstraintFailed, w.name)
	}
	w.txn.Insert(key, k)
	return nil
}

func (w *uniqueIndex) Delete(_ value.Key, r relation.Row) bool {
	key := w.prefix.WithKeys(w.keysForRow(r)...)
	_, ok := w.txn.Delete(key)
	return ok
}

func (w *uniqueIndex) Truncate() {
	w.txn.DeletePrefix(w.prefix)
}

// StandardIndex is an index.Type that allows multiple associations
var StandardIndex = index.Type(
	func(p prefix.Prefix, n index.Name, s relation.Selector) index.Constructor {
		info := makeIndexInfo(p, n, s)
		return func(txn transaction.Txn) index.Index {
			return &standardIndex{
				indexInfo: info,
				txn:       txn,
			}
		}
	},
)

func (i *standardIndex) Insert(k value.Key, r relation.Row) error {
	keys := append(i.keysForRow(r), k)
	key := i.prefix.WithKeys(keys...)
	i.txn.Insert(key, k)
	return nil
}

func (i *standardIndex) Delete(k value.Key, r relation.Row) bool {
	keys := append(i.keysForRow(r), k)
	key := i.prefix.WithKeys(keys...)
	_, ok := i.txn.Delete(key)
	return ok
}

func (i *standardIndex) Truncate() {
	i.txn.DeletePrefix(i.prefix)
}