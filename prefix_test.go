package db_test

import (
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func TestNewSequence(t *testing.T) {
	as := assert.New(t)

	s := db.NewSequence()
	as.NotNil(s)

	p1 := s.Next()
	p2 := s.Next()

	as.Equal(db.Prefix{0, 0, 0, 0}, p1)
	as.Equal(db.Prefix{0, 0, 0, 1}, p2)

	k := db.NewKey()
	combined := append([]byte{0, 0, 0, 1}, k...)
	as.Equal(combined, p2.Bytes(k))
}
