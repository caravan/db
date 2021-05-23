package db

import (
	"github.com/caravan/db/database"
	"github.com/caravan/db/internal"
)

func NewDatabase() database.Database {
	return internal.NewDatabase()
}

var (
	UniqueIndex   = internal.UniqueIndex
	StandardIndex = internal.StandardIndex
)
