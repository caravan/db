package db

import (
	"github.com/caravan/db/database"
	"github.com/caravan/db/internal"
)

// NewDatabase returns a new Database instance
func NewDatabase() database.Database {
	return internal.NewDatabase()
}

var (
	// UniqueIndex is an index.Type that allows only unique associations
	UniqueIndex = internal.UniqueIndex

	// StandardIndex is an index.Type that allows multiple associations
	StandardIndex = internal.StandardIndex
)
