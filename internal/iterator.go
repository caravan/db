package internal

import (
	"sync"

	"github.com/caravan/db/prefix"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"

	radix "github.com/caravan/go-immutable-radix"
)

type (
	forward struct {
		prefix prefix.Prefix
		txn    *radix.Txn
	}

	reverse struct {
		prefix prefix.Prefix
		txn    *radix.Txn
	}

	resolver func() (value.Key, transaction.Any, bool)
)

func makeIter(p prefix.Prefix, fn resolver) transaction.Iterator {
	var once sync.Once
	var k value.Key
	var v transaction.Any
	var next transaction.Iterator
	var ok bool

	return func() (value.Key, transaction.Any, transaction.Iterator, bool) {
		once.Do(func() {
			if k, v, ok = fn(); ok {
				k = k[len(p.Bytes())+1:]
				next = makeIter(p, fn)
			}
		})
		if ok {
			return k, v, next, ok
		}
		return nil, nil, nil, false
	}
}

func (f *forward) All() transaction.Iterator {
	iter := f.txn.Root().Iterator()
	iter.SeekPrefix(append(f.prefix.Bytes(), 0))
	return makeIter(f.prefix, func() (value.Key, transaction.Any, bool) {
		return iter.Next()
	})
}

func (f *forward) From(k value.Key) transaction.Iterator {
	iter := f.txn.Root().Iterator()
	iter.SeekLowerBound(f.prefix.WithKey(k))
	return makeIter(f.prefix, func() (value.Key, transaction.Any, bool) {
		return iter.Next()
	})
}

func (r *reverse) All() transaction.Iterator {
	iter := r.txn.Root().ReverseIterator()
	iter.SeekPrefix(append(r.prefix.Bytes(), 0))
	return makeIter(r.prefix, func() (value.Key, transaction.Any, bool) {
		return iter.Previous()
	})
}

func (r *reverse) From(k value.Key) transaction.Iterator {
	iter := r.txn.Root().ReverseIterator()
	iter.SeekReverseLowerBound(r.prefix.WithKey(k))
	return makeIter(r.prefix, func() (value.Key, transaction.Any, bool) {
		return iter.Previous()
	})
}
