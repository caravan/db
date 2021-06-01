package prefix

import (
	"encoding/binary"

	"github.com/caravan/db/value"
)

type (
	// Prefix is used to partition Database elements within the same
	// underlying data structure
	Prefix uint32

	// Prefixed is a type that is associated with a Prefix
	Prefixed interface {
		Prefix() Prefix
	}
)

// Start is the Prefix zero-value
var Start Prefix

// Prefix makes Prefix a Prefixed instance
func (p Prefix) Prefix() Prefix {
	return p
}

// Next returns the next Prefix in sequence
func (p Prefix) Next() Prefix {
	return p + 1
}

// Bytes returns the underlying byte slice of the Prefix
func (p Prefix) Bytes() []byte {
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, uint32(p))
	return res
}

// WithKey combines this Prefix with a provided Key into a byte array
func (p Prefix) WithKey(k value.Key) value.Key {
	return p.WithKeys(k)
}

// WithKeys combines this Prefix with the provided Keys into a byte array
func (p Prefix) WithKeys(keys ...value.Key) value.Key {
	return value.Key(p.Bytes()).WithKeys(keys...)
}
