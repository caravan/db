package db_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func TestMakeOffsets(t *testing.T) {
	as := assert.New(t)
	c := db.Columns{
		db.MakeColumn("first"),
		db.MakeColumn("second"),
		db.MakeColumn("third"),
	}
	o, err := db.MakeOffsets(c, "second", "third", "first")
	as.NotNil(o)
	as.Nil(err)

	as.Equal(3, len(o))
	as.Equal(db.Offset(1), o[0])
	as.Equal(db.Offset(2), o[1])
	as.Equal(db.Offset(0), o[2])

	o, err = db.MakeOffsets(c, "not there")
	as.Nil(o)
	as.NotNil(err)
	as.EqualError(err, fmt.Sprintf(db.ErrColumnNotFound, "not there"))
}

func TestMakeOffsetSelector(t *testing.T) {
	as := assert.New(t)

	s := db.MakeOffsetSelector(2, 1)
	as.NotNil(s)
	r := s(db.Row{S("first"), S("second"), S("third"), S("fourth")})
	as.NotNil(r)
	as.Equal(2, len(r))
	as.Equal(S("third"), r[0])
	as.Equal(S("second"), r[1])
}

func TestMakeNamedSelector(t *testing.T) {
	as := assert.New(t)
	c := db.Columns{
		db.MakeColumn("first"),
		db.MakeColumn("second"),
		db.MakeColumn("third"),
	}

	s, err := db.MakeNamedSelector(c, "third", "second")
	as.NotNil(s)
	as.Nil(err)

	r := s(db.Row{S("first"), S("second"), S("third"), S("fourth")})
	as.NotNil(r)
	as.Equal(2, len(r))
	as.Equal(S("third"), r[0])
	as.Equal(S("second"), r[1])

	_, err = db.MakeNamedSelector(c, "not there")
	as.NotNil(err)
	as.EqualError(err, fmt.Sprintf(db.ErrColumnNotFound, "not there"))
}

func TestStarSelector(t *testing.T) {
	as := assert.New(t)

	row := db.Row{S("first"), S("second"), S("third"), S("fourth")}
	rel := db.StarSelector(row)
	as.Equal(db.Relation(row), rel)
}
