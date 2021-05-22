package db_test

import (
	"testing"

	"github.com/caravan/essentials/id"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func S(s string) db.String {
	return db.String(s)
}

func TestNewKey(t *testing.T) {
	as := assert.New(t)

	k1 := db.NewKey()
	k2 := db.NewKey()

	b1 := k1.Bytes()
	b2 := k2.Bytes()

	as.NotEqual(k1, k2)
	as.NotEqual(b1, b2)

	as.Equal([]byte(k1), b1)
	as.Equal([]byte(k2), b2)

	nk := db.Key(id.Nil[:])
	as.Equal(db.GreaterThan, k1.Compare(nk))
	as.Equal(db.LessThan, nk.Compare(k1))
	as.Equal(db.EqualTo, k1.Compare(k1))
	as.Equal(db.Incomparable, k1.Compare(S("not a key")))
}

func TestString(t *testing.T) {
	as := assert.New(t)

	s1 := S("first")
	s2 := S("second")

	b1 := s1.Bytes()
	b2 := s2.Bytes()

	as.NotEqual(s1, s2)
	as.NotEqual(b1, b2)

	as.Equal(string(s1), string(b1))
	as.Equal(string(s2), string(b2))

	ns := S("")
	as.Equal(db.GreaterThan, s1.Compare(ns))
	as.Equal(db.LessThan, ns.Compare(s1))
	as.Equal(db.EqualTo, s1.Compare(s1))
	as.Equal(db.Incomparable, s1.Compare(db.NewKey()))
}

func TestBool(t *testing.T) {
	as := assert.New(t)

	b1 := db.Bool(true)
	b2 := db.Bool(false)
	as.NotEqual(b1, b2)

	bb1 := b1.Bytes()
	bb2 := b2.Bytes()
	as.Equal(1, len(bb1))
	as.Equal(uint8(1), bb1[0])
	as.Equal(1, len(bb2))
	as.Equal(uint8(0), bb2[0])

	as.Equal(db.GreaterThan, b1.Compare(b2))
	as.Equal(db.LessThan, b2.Compare(b1))
	as.Equal(db.EqualTo, b1.Compare(b1))
	as.Equal(db.Incomparable, b1.Compare(db.NewKey()))
}

func TestInteger(t *testing.T) {
	as := assert.New(t)

	i1 := db.Integer(-1)
	i2 := db.Integer(1)

	b1 := i1.Bytes()
	b2 := i2.Bytes()

	as.NotEqual(i1, i2)
	as.NotEqual(b1, b2)

	as.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1}, b1)
	as.Equal([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}, b2)

	ni := db.Integer(-100)
	as.Equal(db.GreaterThan, i1.Compare(ni))
	as.Equal(db.LessThan, ni.Compare(i1))
	as.Equal(db.EqualTo, i1.Compare(i1))
	as.Equal(db.Incomparable, i1.Compare(db.NewKey()))
}

func TestFloat(t *testing.T) {
	as := assert.New(t)

	f1 := db.Float(42)
	f2 := db.Float(96)

	b1 := f1.Bytes()
	b2 := f2.Bytes()

	as.NotEqual(f1, f2)
	as.NotEqual(b1, b2)

	nf := db.Float(0)
	as.Equal(db.GreaterThan, f1.Compare(nf))
	as.Equal(db.LessThan, nf.Compare(f1))
	as.Equal(db.EqualTo, f1.Compare(f1))
	as.Equal(db.Incomparable, f1.Compare(db.NewKey()))
}
