package internal_test

import (
	"testing"

	"github.com/caravan/db/internal"
	"github.com/caravan/db/prefix"
	"github.com/caravan/db/value"
	"github.com/stretchr/testify/assert"

	iradix "github.com/caravan/go-immutable-radix"
)

func TestForwardIterableAll(t *testing.T) {
	as := assert.New(t)

	pfx := prefix.Start
	tree := iradix.New()
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(4).Bytes()), 3)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(1).Bytes()), 1)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(8).Bytes()), 4)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(2).Bytes()), 2)

	next := internal.MakeForwardIterable(pfx, tree.Txn()).All()
	k, v, next, ok := next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}), k)
	as.Equal(1, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2}), k)
	as.Equal(2, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 4}), k)
	as.Equal(3, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 8}), k)
	as.Equal(4, v)

	k, v, next, ok = next()
	as.False(ok)
	as.Nil(k)
	as.Nil(v)
	as.Nil(next)
}

func TestForwardIterableFrom(t *testing.T) {
	as := assert.New(t)

	pfx := prefix.Start
	tree := iradix.New()
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(4).Bytes()), 3)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(1).Bytes()), 1)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(8).Bytes()), 4)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(2).Bytes()), 2)

	next := internal.
		MakeForwardIterable(pfx, tree.Txn()).
		From(value.Integer(4).Bytes())
	k, v, next, ok := next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 4}), k)
	as.Equal(3, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 8}), k)
	as.Equal(4, v)

	k, v, next, ok = next()
	as.False(ok)
	as.Nil(k)
	as.Nil(v)
	as.Nil(next)
}

func TestReverseIterableAll(t *testing.T) {
	as := assert.New(t)

	pfx := prefix.Start
	tree := iradix.New()
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(4).Bytes()), 3)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(1).Bytes()), 1)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(8).Bytes()), 4)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(2).Bytes()), 2)

	next := internal.MakeReverseIterable(pfx, tree.Txn()).All()
	k, v, next, ok := next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 8}), k)
	as.Equal(4, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 4}), k)
	as.Equal(3, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2}), k)
	as.Equal(2, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}), k)
	as.Equal(1, v)

	k, v, next, ok = next()
	as.False(ok)
	as.Nil(k)
	as.Nil(v)
	as.Nil(next)
}

func TestReverseIterableFrom(t *testing.T) {
	as := assert.New(t)

	pfx := prefix.Start
	tree := iradix.New()
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(4).Bytes()), 3)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(1).Bytes()), 1)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(8).Bytes()), 4)
	tree, _, _ = tree.Insert(pfx.WithKey(value.Integer(2).Bytes()), 2)

	next := internal.
		MakeReverseIterable(pfx, tree.Txn()).
		From(value.Integer(3).Bytes())
	k, v, next, ok := next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2}), k)
	as.Equal(2, v)

	k, v, next, ok = next()
	as.True(ok)
	as.Equal(value.Key([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}), k)
	as.Equal(1, v)

	k, v, next, ok = next()
	as.False(ok)
	as.Nil(k)
	as.Nil(v)
	as.Nil(next)
}
