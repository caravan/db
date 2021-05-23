package relation_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db/column"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/value"
	"github.com/stretchr/testify/assert"
)

func TestMakeOffsets(t *testing.T) {
	as := assert.New(t)
	c := column.Columns{
		column.Make("first"),
		column.Make("second"),
		column.Make("third"),
	}
	o, err := relation.MakeOffsets(c, "second", "third", "first")
	as.NotNil(o)
	as.Nil(err)

	as.Equal(3, len(o))
	as.Equal(column.Offset(1), o[0])
	as.Equal(column.Offset(2), o[1])
	as.Equal(column.Offset(0), o[2])

	o, err = relation.MakeOffsets(c, "not there")
	as.Nil(o)
	as.NotNil(err)
	as.EqualError(err, fmt.Sprintf(relation.ErrColumnNotFound, "not there"))
}

func TestMakeOffsetSelector(t *testing.T) {
	as := assert.New(t)

	s := relation.MakeOffsetSelector(2, 1)
	as.NotNil(s)
	r := s(relation.Row{
		value.String("first"),
		value.String("second"),
		value.String("third"),
		value.String("fourth"),
	})
	as.NotNil(r)
	as.Equal(2, len(r))
	as.Equal(value.String("third"), r[0])
	as.Equal(value.String("second"), r[1])
}

func TestMakeNamedSelector(t *testing.T) {
	as := assert.New(t)
	c := column.Columns{
		column.Make("first"),
		column.Make("second"),
		column.Make("third"),
	}

	s, err := relation.MakeNamedSelector(c, "third", "second")
	as.NotNil(s)
	as.Nil(err)

	r := s(relation.Row{
		value.String("first"),
		value.String("second"),
		value.String("third"),
		value.String("fourth"),
	})
	as.NotNil(r)
	as.Equal(2, len(r))
	as.Equal(value.String("third"), r[0])
	as.Equal(value.String("second"), r[1])

	_, err = relation.MakeNamedSelector(c, "not there")
	as.NotNil(err)
	as.EqualError(err, fmt.Sprintf(relation.ErrColumnNotFound, "not there"))
}

func TestStarSelector(t *testing.T) {
	as := assert.New(t)

	row := relation.Row{
		value.String("first"),
		value.String("second"),
		value.String("third"),
		value.String("fourth"),
	}
	rel := relation.StarSelector(row)
	as.Equal(relation.Relation(row), rel)
}
