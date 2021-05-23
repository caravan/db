package column

type (
	// Name identifies a Column
	Name string

	// Names are a set of Name
	Names []Name

	// Column describes a column to be selected from a Table. The
	// description includes the column's name and a TableSelector for
	// retrieving the column's value from an Event
	Column interface {
		Name() Name
	}

	// Columns are a set of Column
	Columns []Column

	// Offset is the location of a Column within a set of Columns
	Offset int

	// Offsets are a set of Offset
	Offsets []Offset

	// NamedOffsets allows an Offset to be retrieved by Name
	NamedOffsets map[Name]Offset

	// column is the internal implementation of a column
	column struct {
		name Name
	}
)

// Make instantiates a new column instance
func Make(n Name) Column {
	return &column{
		name: n,
	}
}

func (c *column) Name() Name {
	return c.name
}

// MakeNamedOffsets takes a set of Columns and returns its NamedOffsets
func MakeNamedOffsets(cols ...Column) NamedOffsets {
	res := make(NamedOffsets, len(cols))
	for i, c := range cols {
		res[c.Name()] = Offset(i)
	}
	return res
}
