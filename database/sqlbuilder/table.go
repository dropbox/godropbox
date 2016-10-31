// Modeling of tables.  This is where query preparation starts

package sqlbuilder

import (
	"bytes"
	"fmt"

	"github.com/dropbox/godropbox/errors"
)

// The sql table read interface.  NOTE: NATURAL JOINs, and join "USING" clause
// are not supported.
type ReadableTable interface {
	// Returns the list of columns that are in the current table expression.
	Columns() []NonAliasColumn

	// Generates the sql string for the current table expression.  Note: the
	// generated string may not be a valid/executable sql statement.
	// The database is the name of the database the table is on
	SerializeSql(database string, out *bytes.Buffer) error

	// Generates a select query on the current table.
	Select(projections ...Projection) SelectStatement

	// Creates a inner join table expression using onCondition.
	InnerJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a left join table expression using onCondition.
	LeftJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable

	// Creates a right join table expression using onCondition.
	RightJoinOn(table ReadableTable, onCondition BoolExpression) ReadableTable
}

// The sql table write interface.
type WritableTable interface {
	// Returns the list of columns that are in the table.
	Columns() []NonAliasColumn

	// Generates the sql string for the current table expression.  Note: the
	// generated string may not be a valid/executable sql statement.
	// The database is the name of the database the table is on
	SerializeSql(database string, out *bytes.Buffer) error

	Insert(columns ...NonAliasColumn) InsertStatement
	Update() UpdateStatement
	Delete() DeleteStatement
}

// Defines a physical table in the database that is both readable and writable.
// This function will panic if name is not valid
func NewTable(name string, columns ...NonAliasColumn) *Table {
	if !validIdentifierName(name) {
		panic("Invalid table name")
	}

	t := &Table{
		name:         name,
		columns:      columns,
		columnLookup: make(map[string]NonAliasColumn),
	}
	for _, c := range columns {
		err := c.setTableName(name)
		if err != nil {
			panic(err)
		}
		t.columnLookup[c.Name()] = c
	}

	if len(columns) == 0 {
		panic(fmt.Sprintf("Table %s has no columns", name))
	}

	return t
}

type Table struct {
	name         string
	columns      []NonAliasColumn
	columnLookup map[string]NonAliasColumn
	// If not empty, the name of the index to force
	forcedIndex string
}

// Returns the specified column, or errors if it doesn't exist in the table
func (t *Table) getColumn(name string) (NonAliasColumn, error) {
	if c, ok := t.columnLookup[name]; ok {
		return c, nil
	}
	return nil, errors.Newf("No such column '%s' in table '%s'", name, t.name)
}

// Returns a pseudo column representation of the column name.  Error checking
// is deferred to SerializeSql.
func (t *Table) C(name string) NonAliasColumn {
	return &deferredLookupColumn{
		table:   t,
		colName: name,
	}
}

// Returns all columns for a table as a slice of projections
func (t *Table) Projections() []Projection {
	result := make([]Projection, 0)

	for _, col := range t.columns {
		result = append(result, col)
	}

	return result
}

// Returns the table's name in the database
func (t *Table) Name() string {
	return t.name
}

// Returns a list of the table's columns
func (t *Table) Columns() []NonAliasColumn {
	return t.columns
}

// Returns a copy of this table, but with the specified index forced.
func (t *Table) ForceIndex(index string) *Table {
	newTable := *t
	newTable.forcedIndex = index
	return &newTable
}

// Generates the sql string for the current table expression.  Note: the
// generated string may not be a valid/executable sql statement.
func (t *Table) SerializeSql(database string, out *bytes.Buffer) error {
	_, _ = out.WriteString("`")
	_, _ = out.WriteString(database)
	_, _ = out.WriteString("`.`")
	_, _ = out.WriteString(t.Name())
	_, _ = out.WriteString("`")

	if t.forcedIndex != "" {
		if !validIdentifierName(t.forcedIndex) {
			return errors.Newf("'%s' is not a valid identifier for an index", t.forcedIndex)
		}
		_, _ = out.WriteString(" FORCE INDEX (`")
		_, _ = out.WriteString(t.forcedIndex)
		_, _ = out.WriteString("`)")
	}

	return nil
}

// Generates a select query on the current table.
func (t *Table) Select(projections ...Projection) SelectStatement {
	return newSelectStatement(t, projections)
}

// Creates a inner join table expression using onCondition.
func (t *Table) InnerJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return InnerJoinOn(t, table, onCondition)
}

// Creates a left join table expression using onCondition.
func (t *Table) LeftJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return LeftJoinOn(t, table, onCondition)
}

// Creates a right join table expression using onCondition.
func (t *Table) RightJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return RightJoinOn(t, table, onCondition)
}

func (t *Table) Insert(columns ...NonAliasColumn) InsertStatement {
	return newInsertStatement(t, columns...)
}

func (t *Table) Update() UpdateStatement {
	return newUpdateStatement(t)
}

func (t *Table) Delete() DeleteStatement {
	return newDeleteStatement(t)
}

type joinType int

const (
	INNER_JOIN joinType = iota
	LEFT_JOIN
	RIGHT_JOIN
)

// Join expressions are pseudo readable tables.
type joinTable struct {
	lhs         ReadableTable
	rhs         ReadableTable
	join_type   joinType
	onCondition BoolExpression
}

func newJoinTable(
	lhs ReadableTable,
	rhs ReadableTable,
	join_type joinType,
	onCondition BoolExpression) ReadableTable {

	return &joinTable{
		lhs:         lhs,
		rhs:         rhs,
		join_type:   join_type,
		onCondition: onCondition,
	}
}

func InnerJoinOn(
	lhs ReadableTable,
	rhs ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return newJoinTable(lhs, rhs, INNER_JOIN, onCondition)
}

func LeftJoinOn(
	lhs ReadableTable,
	rhs ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return newJoinTable(lhs, rhs, LEFT_JOIN, onCondition)
}

func RightJoinOn(
	lhs ReadableTable,
	rhs ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return newJoinTable(lhs, rhs, RIGHT_JOIN, onCondition)
}

func (t *joinTable) Columns() []NonAliasColumn {
	columns := make([]NonAliasColumn, 0)
	columns = append(columns, t.lhs.Columns()...)
	columns = append(columns, t.rhs.Columns()...)

	return columns
}

func (t *joinTable) SerializeSql(
	database string,
	out *bytes.Buffer) (err error) {

	if t.lhs == nil {
		return errors.Newf("nil lhs.  Generated sql: %s", out.String())
	}
	if t.rhs == nil {
		return errors.Newf("nil rhs.  Generated sql: %s", out.String())
	}
	if t.onCondition == nil {
		return errors.Newf("nil onCondition.  Generated sql: %s", out.String())
	}

	if err = t.lhs.SerializeSql(database, out); err != nil {
		return
	}

	switch t.join_type {
	case INNER_JOIN:
		_, _ = out.WriteString(" JOIN ")
	case LEFT_JOIN:
		_, _ = out.WriteString(" LEFT JOIN ")
	case RIGHT_JOIN:
		_, _ = out.WriteString(" RIGHT JOIN ")
	}

	if err = t.rhs.SerializeSql(database, out); err != nil {
		return
	}

	_, _ = out.WriteString(" ON ")
	if err = t.onCondition.SerializeSql(out); err != nil {
		return
	}

	return nil
}

func (t *joinTable) Select(projections ...Projection) SelectStatement {
	return newSelectStatement(t, projections)
}

func (t *joinTable) InnerJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return InnerJoinOn(t, table, onCondition)
}

func (t *joinTable) LeftJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return LeftJoinOn(t, table, onCondition)
}

func (t *joinTable) RightJoinOn(
	table ReadableTable,
	onCondition BoolExpression) ReadableTable {

	return RightJoinOn(t, table, onCondition)
}
