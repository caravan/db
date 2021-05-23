package db_test

import (
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func TestNewDatabase(t *testing.T) {
	as := assert.New(t)
	d := db.NewDatabase()
	as.NotNil(d)
}
