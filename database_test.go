package db_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func TestMakeDatabase(t *testing.T) {
	as := assert.New(t)
	d := db.NewDatabase()
	as.NotNil(d)
}

func TestCreateTransaction(t *testing.T) {
	as := assert.New(t)
	d := db.NewDatabase()
	tx := d.CreateTransaction()
	as.NotNil(tx)
}

func TestCreateTable(t *testing.T) {
	as := assert.New(t)
	d := db.NewDatabase()

	tbl, err := d.CreateTable("my-table")
	as.NotNil(tbl)
	as.Nil(err)

	_, err = d.CreateTable("my-table")
	as.NotNil(err)
	as.EqualError(err, fmt.Sprintf(db.ErrTableAlreadyExists, "my-table"))

	l := d.Tables()
	as.Equal(1, len(l))
	as.Equal(db.TableName("my-table"), l[0])

	tbl2, ok := d.Table(l[0])
	as.NotNil(tbl2)
	as.True(ok)
	as.Equal(tbl, tbl2)
}
