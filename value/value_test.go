package value_test

import (
	"testing"

	"github.com/caravan/db/value"
	"github.com/caravan/essentials/id"
	"github.com/stretchr/testify/assert"
)

func TestNewKey(t *testing.T) {
	as := assert.New(t)

	k1 := value.NewKey()
	k2 := value.NewKey()

	b1 := k1.Bytes()
	b2 := k2.Bytes()

	as.NotEqual(k1, k2)
	as.NotEqual(b1, b2)

	as.Equal([]byte(k1), b1)
	as.Equal([]byte(k2), b2)

	nk := value.Key(id.Nil[:])
	as.Equal(value.GreaterThan, k1.Compare(nk))
	as.Equal(value.LessThan, nk.Compare(k1))
	as.Equal(value.EqualTo, k1.Compare(k1))
	as.Equal(value.Incomparable, k1.Compare(value.String("not a key")))
}

func TestString(t *testing.T) {
	as := assert.New(t)

	s1 := value.String("first")
	s2 := value.String("second")

	b1 := s1.Bytes()
	b2 := s2.Bytes()

	as.NotEqual(s1, s2)
	as.NotEqual(b1, b2)

	as.Equal(string(s1), string(b1))
	as.Equal(string(s2), string(b2))

	ns := value.String("")
	as.Equal(value.GreaterThan, s1.Compare(ns))
	as.Equal(value.LessThan, ns.Compare(s1))
	as.Equal(value.EqualTo, s1.Compare(s1))
	as.Equal(value.Incomparable, s1.Compare(value.NewKey()))
}

func TestBool(t *testing.T) {
	as := assert.New(t)

	b1 := value.Bool(true)
	b2 := value.Bool(false)
	as.NotEqual(b1, b2)

	bb1 := b1.Bytes()
	bb2 := b2.Bytes()
	as.Equal(1, len(bb1))
	as.Equal(uint8(1), bb1[0])
	as.Equal(1, len(bb2))
	as.Equal(uint8(0), bb2[0])

	as.Equal(value.GreaterThan, b1.Compare(b2))
	as.Equal(value.LessThan, b2.Compare(b1))
	as.Equal(value.EqualTo, b1.Compare(b1))
	as.Equal(value.Incomparable, b1.Compare(value.NewKey()))
}

func TestInteger(t *testing.T) {
	as := assert.New(t)

	i1 := value.Integer(-1)
	i2 := value.Integer(1)

	b1 := i1.Bytes()
	b2 := i2.Bytes()

	as.NotEqual(i1, i2)
	as.NotEqual(b1, b2)

	as.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1}, b1)
	as.Equal([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}, b2)

	ni := value.Integer(-100)
	as.Equal(value.GreaterThan, i1.Compare(ni))
	as.Equal(value.LessThan, ni.Compare(i1))
	as.Equal(value.EqualTo, i1.Compare(i1))
	as.Equal(value.Incomparable, i1.Compare(value.NewKey()))
}

func TestFloat(t *testing.T) {
	as := assert.New(t)

	f1 := value.Float(42)
	f2 := value.Float(96)

	b1 := f1.Bytes()
	b2 := f2.Bytes()

	as.NotEqual(f1, f2)
	as.NotEqual(b1, b2)

	nf := value.Float(0)
	as.Equal(value.GreaterThan, f1.Compare(nf))
	as.Equal(value.LessThan, nf.Compare(f1))
	as.Equal(value.EqualTo, f1.Compare(f1))
	as.Equal(value.Incomparable, f1.Compare(value.NewKey()))
}
