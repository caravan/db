package column_test

import (
	"testing"

	"github.com/caravan/db/column"
	"github.com/stretchr/testify/assert"
)

func TestColumn(t *testing.T) {
	as := assert.New(t)

	c := column.Make("some-col")
	as.Equal(column.Name("some-col"), c.Name())
}

func TestMakeNamedOffsets(t *testing.T) {
	as := assert.New(t)

	c := column.Columns{
		column.Make("first"),
		column.Make("second"),
		column.Make("third"),
	}

	off := column.MakeNamedOffsets(c...)
	as.NotNil(off)
	as.Equal(3, len(off))
	as.Equal(column.Offset(2), off["third"])
	as.Equal(column.Offset(1), off["second"])
	as.Equal(column.Offset(0), off["first"])
}
