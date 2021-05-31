package iterate_test

import (
	"errors"
	"testing"

	"github.com/caravan/db/transaction"
	"github.com/caravan/db/transaction/iterate"
	"github.com/caravan/db/value"
	"github.com/stretchr/testify/assert"
)

func makeSequence(start int) transaction.Iterator {
	return func() (value.Key, transaction.Any, transaction.Iterator, bool) {
		return []byte{}, start, makeSequence(start + 1), true
	}
}

func TestForEach(t *testing.T) {
	as := assert.New(t)
	err := iterate.ForEach(makeSequence(0),
		func(_ value.Key, v transaction.Any) error {
			if v, ok := v.(int); ok {
				if v > 10 {
					return errors.New("done")
				}
				return nil
			}
			panic("not an int")
		},
	)
	as.NotNil(err)
	as.EqualError(err, "done")
}

func TestWhile(t *testing.T) {
	as := assert.New(t)
	iter := iterate.While(makeSequence(0),
		func(_ value.Key, v transaction.Any) bool {
			res, ok := v.(int)
			return ok && res < 3
		},
	)

	_, v, next, ok := iter()
	as.True(ok)
	as.Equal(0, v)

	_, v, next, ok = next()
	as.True(ok)
	as.Equal(1, v)

	_, v, next, ok = next()
	as.True(ok)
	as.Equal(2, v)

	_, v, next, ok = next()
	as.False(ok)
	as.Nil(v)
	as.Nil(next)
}
