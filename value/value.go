package value

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/caravan/essentials/id"
)

type (
	// Value is a placeholder for what will eventually be a generic
	Value interface {
		Compare(Value) Comparison
		Bytes() []byte
	}

	// Comparison represents the result of an equality comparison
	Comparison int

	// Key is a Value that represents a database key
	Key []byte

	// Bool is a Value that represents a stored boolean
	Bool bool

	// String is a Value that represents a stored string
	String string

	// Integer is a Value that represents a stored integer
	Integer int64

	// Float is a Value that represents a stored floating point number
	Float float64
)

// Comparison results
const (
	LessThan Comparison = iota - 1
	EqualTo
	GreaterThan
	Incomparable
)

var (
	trueBytes  = []byte{1}
	falseBytes = []byte{0}
	emptyKey   = Key{}
)

// NewKey returns a new unique database Key
func NewKey() Key {
	return id.New().Bytes()
}

// Compare returns a Comparison between this Key and another Value
func (l Key) Compare(r Value) Comparison {
	if r, ok := r.(Key); ok {
		ls := string(l)
		rs := string(r)
		switch {
		case ls == rs:
			return EqualTo
		case ls < rs:
			return LessThan
		default:
			return GreaterThan
		}
	}
	return Incomparable
}

// Bytes returns a byte-array representation of this Key
func (l Key) Bytes() []byte {
	return l
}

// WithKeys combines a Key with a set of additional Keys
func (l Key) WithKeys(k ...Key) Key {
	keys := append([]Key{l}, k...)
	return JoinKeys(keys...)
}

// JoinKeys joins a set of Keys or returns an empty Key if provided none
func JoinKeys(keys ...Key) Key {
	if len(keys) == 0 {
		return emptyKey
	}
	var buf bytes.Buffer
	buf.Write(keys[0].Bytes())
	for _, k := range keys[1:] {
		buf.WriteByte(0)
		buf.Write(k.Bytes())
	}
	return buf.Bytes()
}

// Compare returns a Comparison between this Bool and another Value
func (l Bool) Compare(r Value) Comparison {
	if r, ok := r.(Bool); ok {
		switch {
		case l == r:
			return EqualTo
		case l == false:
			return LessThan
		default:
			return GreaterThan
		}
	}
	return Incomparable
}

// Bytes returns a byte-array representation of this Bool
func (l Bool) Bytes() []byte {
	if l {
		return trueBytes
	}
	return falseBytes
}

// Compare returns a Comparison between this String and another Value
func (l String) Compare(r Value) Comparison {
	if r, ok := r.(String); ok {
		switch {
		case l == r:
			return EqualTo
		case l < r:
			return LessThan
		default:
			return GreaterThan
		}
	}
	return Incomparable
}

// Bytes returns a byte-array representation of this String
func (l String) Bytes() []byte {
	return []byte(l)
}

// Compare returns a Comparison between this Integer and another Value
func (l Integer) Compare(r Value) Comparison {
	if r, ok := r.(Integer); ok {
		switch {
		case l == r:
			return EqualTo
		case l < r:
			return LessThan
		default:
			return GreaterThan
		}
	}
	return Incomparable
}

// Bytes returns a byte-array representation of this Integer
func (l Integer) Bytes() []byte {
	var buf bytes.Buffer
	holder := make([]byte, 8)
	if l >= 0 {
		buf.WriteByte(1)
		binary.BigEndian.PutUint64(holder, uint64(l))
		buf.Write(holder)
	} else {
		buf.WriteByte(0)
		binary.BigEndian.PutUint64(holder, uint64(-l))
		buf.Write(holder)
	}
	return buf.Bytes()
}

// Compare returns a Comparison between this Float and another Value
func (l Float) Compare(r Value) Comparison {
	if r, ok := r.(Float); ok {
		switch {
		case l == r:
			return EqualTo
		case l < r:
			return LessThan
		default:
			return GreaterThan
		}
	}
	return Incomparable
}

// Bytes returns a byte-array representation of this Float
func (l Float) Bytes() []byte {
	var buf bytes.Buffer
	i := int64(l)
	buf.Write(Integer(i).Bytes())

	u := math.Float64bits(float64(l) - float64(i))
	holder := make([]byte, 8)
	binary.BigEndian.PutUint64(holder, u)
	buf.Write(holder)
	return buf.Bytes()
}
