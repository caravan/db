package db

import (
	"encoding/binary"
	"sync"
)

type (
	// Sequence manages a set of automatically incrementing Prefix
	Sequence interface {
		Next() Prefix
	}

	// Prefix is used to partition Database elements within the same
	// underlying data structure
	Prefix []byte

	// sequence is the internal implementation of a Sequence
	sequence struct {
		sync.Mutex
		next uint32
	}
)

// NewSequence returns a new instance of Sequence
func NewSequence() Sequence {
	return &sequence{}
}

func (p *sequence) Next() Prefix {
	p.Lock()
	defer p.Unlock()

	res := make(Prefix, 4)
	seq := p.next
	p.next++
	binary.BigEndian.PutUint32(res, seq)
	return res
}

// Bytes combines this Prefix with a provided Key into a byte array
func (p Prefix) Bytes(k Key) []byte {
	b := k.Bytes()
	res := make([]byte, 0, len(p)+len(b))
	res = append(res, p...)
	res = append(res, b...)
	return res
}
