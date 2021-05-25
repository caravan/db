package prefix

import (
	"bytes"
	"encoding/binary"

	"github.com/caravan/db/value"
)

// Prefix is used to partition Database elements within the same
// underlying data structure
type Prefix uint32

// Start is the Prefix zero-value
var Start Prefix

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
func (p Prefix) WithKey(k value.Key) []byte {
	return p.WithKeys(k)
}

// WithKeys combines this Prefix with the provided Keys into a byte array
func (p Prefix) WithKeys(keys ...value.Key) []byte {
	var buf bytes.Buffer
	buf.Write(p.Bytes())
	for _, k := range keys {
		buf.WriteByte(0)
		buf.Write(k.Bytes())
	}
	return buf.Bytes()
}
