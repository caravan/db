package internal_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db/database"
	"github.com/caravan/db/internal"
	"github.com/caravan/db/table"
	"github.com/stretchr/testify/assert"
)

func TestMakeDatabase(t *testing.T) {
	as := assert.New(t)
	d := internal.NewDatabase()
	as.NotNil(d)
}

func TestCreateTable(t *testing.T) {
	as := assert.New(t)
	d := internal.NewDatabase()
	as.Nil(d(func(d database.Database) error {
		tbl, err := d.CreateTable("test-table")
		as.NotNil(tbl)
		as.Nil(err)

		_, err = d.CreateTable("test-table")
		as.NotNil(err)
		as.EqualError(err,
			fmt.Sprintf(internal.ErrTableAlreadyExists, "test-table"),
		)

		l := d.Tables()
		as.Equal(1, len(l))
		as.Equal(table.Name("test-table"), l[0])

		tbl2, ok := d.Table(l[0])
		as.NotNil(tbl2)
		as.True(ok)
		as.Equal(tbl, tbl2)
		return nil
	}))
}
