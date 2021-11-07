package iterate

import (
	"github.com/caravan/db/transaction"
	"github.com/caravan/db/value"
)

type (
	// Reporter is called to report a key/value pair to iterating code
	Reporter func(value.Key, transaction.Any) error

	// Predicate is called to determine whether to report a key/value pair
	Predicate func(value.Key, transaction.Any) bool
)

// ForEach iterates over a transaction.Iterator and calls a Reporter for each
// pair. If that Reporter returns an error, the iteration is canceled and the
// error is returned
func ForEach(iter transaction.Iterator, fn Reporter) error {
	for k, v, next, ok := iter(); ok; k, v, next, ok = next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// While iterates over a transaction.Iterator and checks its pairs
// against the provided Predicate. The iteration is canceled the first
// time the Predicate returns false
func While(iter transaction.Iterator, fn Predicate) transaction.Iterator {
	return func() (value.Key, transaction.Any, transaction.Iterator, bool) {
		k, v, next, ok := iter()
		if ok && fn(k, v) {
			return k, v, While(next, fn), ok
		}
		return nil, nil, nil, false
	}
}
