package db_test

import (
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func TestColumn(t *testing.T) {
	as := assert.New(t)

	c := db.MakeColumn("some-col")
	as.Equal(db.ColumnName("some-col"), c.Name())
}

func TestMakeNamedOffsets(t *testing.T) {
	as := assert.New(t)

	c := db.Columns{
		db.MakeColumn("first"),
		db.MakeColumn("second"),
		db.MakeColumn("third"),
	}

	off := db.MakeNamedOffsets(c...)
	as.NotNil(off)
	as.Equal(3, len(off))
	as.Equal(db.Offset(2), off["third"])
	as.Equal(db.Offset(1), off["second"])
	as.Equal(db.Offset(0), off["first"])
}
