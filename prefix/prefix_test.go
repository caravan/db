package prefix_test

import (
	"testing"

	"github.com/caravan/db/prefix"
	"github.com/caravan/db/value"
	"github.com/stretchr/testify/assert"
)

func TestNewSequence(t *testing.T) {
	as := assert.New(t)

	s := prefix.NewSequence()
	as.NotNil(s)

	p1 := s.Next()
	p2 := s.Next()

	as.Equal(prefix.Prefix{0, 0, 0, 0}, p1)
	as.Equal(prefix.Prefix{0, 0, 0, 1}, p2)

	k := value.NewKey()
	combined := append([]byte{0, 0, 0, 1}, k...)
	as.Equal(combined, p2.Bytes(k))
}
