package internal

import (
	"sync"

	"github.com/caravan/db/prefix"
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"

	radix "github.com/hashicorp/go-immutable-radix/v2"
)

type (
	iterable struct {
		prefix.Prefixed
		*radix.Txn[any]
	}

	forwardIterable struct{ iterable }
	reverseIterable struct{ iterable }

	resolver func() (value.Key, any, bool)
)

func (s iterable) start() int {
	return len(s.Prefix().Bytes()) + 1
}

func (s iterable) resolved(fn resolver) transaction.Iterator {
	var once sync.Once
	var k value.Key
	var v any
	var next transaction.Iterator
	var ok bool

	return func() (value.Key, any, transaction.Iterator, bool) {
		once.Do(func() {
			if k, v, ok = fn(); ok {
				k = k[s.start():]
				next = s.resolved(fn)
			}
		})
		if ok {
			return k, v, next, ok
		}
		return nil, nil, nil, false
	}
}

// ForwardIterable constructs an ascending iterable interface
func ForwardIterable(
	p prefix.Prefixed, t *radix.Txn[any],
) transaction.Iterable {
	return &forwardIterable{
		iterable{
			Prefixed: p,
			Txn:      t,
		},
	}
}

func (f *forwardIterable) All() transaction.Iterator {
	iter := f.Txn.Root().Iterator()
	iter.SeekPrefix(append(f.Prefix().Bytes(), 0))
	return f.resolved(func() (value.Key, any, bool) {
		return iter.Next()
	})
}

func (f *forwardIterable) From(k value.Key) transaction.Iterator {
	iter := f.Txn.Root().Iterator()
	iter.SeekLowerBound(f.Prefix().WithKey(k))
	return f.resolved(func() (value.Key, any, bool) {
		return iter.Next()
	})
}

// ReverseIterable constructs a descending iterable interface
func ReverseIterable(
	p prefix.Prefixed, t *radix.Txn[any],
) transaction.Iterable {
	return &reverseIterable{
		iterable{
			Prefixed: p,
			Txn:      t,
		},
	}
}

func (r *reverseIterable) All() transaction.Iterator {
	iter := r.Txn.Root().ReverseIterator()
	iter.SeekPrefix(append(r.Prefix().Bytes(), 0))
	return r.resolved(func() (value.Key, any, bool) {
		return iter.Previous()
	})
}

func (r *reverseIterable) From(k value.Key) transaction.Iterator {
	iter := r.Txn.Root().ReverseIterator()
	iter.SeekReverseLowerBound(r.Prefix().WithKey(k))
	return r.resolved(func() (value.Key, any, bool) {
		return iter.Previous()
	})
}
