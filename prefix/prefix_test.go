package prefix_test

import (
	"testing"

	"github.com/caravan/db/value"

	"github.com/caravan/db/prefix"
	"github.com/stretchr/testify/assert"
)

func TestPrefixes(t *testing.T) {
	as := assert.New(t)

	s := prefix.Start
	as.NotNil(s)

	p1 := s.Next()
	p2 := p1.Next()

	as.NotEqual(prefix.Start, p1)
	as.NotEqual(p1, p2)

	as.Equal([]byte{0, 0, 0, 0}, s.Bytes())
	as.Equal([]byte{0, 0, 0, 1}, p1.Bytes())
	as.Equal([]byte{0, 0, 0, 2}, p2.Bytes())

	as.Equal(value.Key{0, 0, 0, 2, 0, 1}, p2.WithKey([]byte{1}))
	as.Equal(value.Key{0, 0, 0, 2, 0, 1, 0, 2}, p2.WithKeys([]byte{1}, []byte{2}))

	var p3 prefix.Prefixed = p2.Next()
	as.Equal(p3, p2.Next())
	as.Equal(p3.Prefix(), p3)
}
