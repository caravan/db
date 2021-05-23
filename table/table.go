package table

import (
	"github.com/caravan/db/column"
	"github.com/caravan/db/index"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/value"
)

type (
	// Name identifies a Table
	Name string

	// Names are a set of Name
	Names []Name

	// Table is an interface that associates a Key with a Row and provides
	// additional capabilities around this association
	Table interface {
		Name() Name
		Columns() column.Columns

		MutateWith(MutatorFunc) error

		CreateIndex(index.Type, index.Name, ...column.Name) (index.Index, error)
		Indexes() index.Names
		Index(index.Name) (index.Index, bool)
	}

	// MutatorFunc is provided in order to sequence Table mutations
	MutatorFunc func(Mutator) error

	// Mutator allows modification of the internal state of a Table
	Mutator interface {
		Truncate()
		Insert(value.Key, relation.Row) error
		Update(value.Key, relation.Row) (relation.Row, error)
		Delete(value.Key) (relation.Row, bool)
		Select(value.Key) (relation.Row, bool)
	}
)
