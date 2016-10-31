// Modeling of columns

package sqlbuilder

import (
	"bytes"
	"regexp"

	"github.com/dropbox/godropbox/errors"
)

// XXX: Maybe add UIntColumn

// Representation of a table for query generation
type Column interface {
	isProjectionInterface

	Name() string
	// Serialization for use in column lists
	SerializeSqlForColumnList(out *bytes.Buffer) error
	// Serialization for use in an expression (Clause)
	SerializeSql(out *bytes.Buffer) error

	// Internal function for tracking table that a column belongs to
	// for the purpose of serialization
	setTableName(table string) error
}

type NullableColumn bool

const (
	Nullable    NullableColumn = true
	NotNullable NullableColumn = false
)

// A column that can be refer to outside of the projection list
type NonAliasColumn interface {
	Column
	isOrderByClauseInterface
	isExpressionInterface
}

type Collation string

const (
	UTF8CaseInsensitive Collation = "utf8_unicode_ci"
	UTF8CaseSensitive   Collation = "utf8_unicode"
	UTF8Binary          Collation = "utf8_bin"
)

// Representation of MySQL charsets
type Charset string

const (
	UTF8 Charset = "utf8"
)

// The base type for real materialized columns.
type baseColumn struct {
	isProjection
	isExpression
	name     string
	nullable NullableColumn
	table    string
}

func (c *baseColumn) Name() string {
	return c.name
}

func (c *baseColumn) setTableName(table string) error {
	c.table = table
	return nil
}

func (c *baseColumn) SerializeSqlForColumnList(out *bytes.Buffer) error {
	if c.table != "" {
		_ = out.WriteByte('`')
		_, _ = out.WriteString(c.table)
		_, _ = out.WriteString("`.")
	}
	_, _ = out.WriteString("`")
	_, _ = out.WriteString(c.name)
	_ = out.WriteByte('`')
	return nil
}

func (c *baseColumn) SerializeSql(out *bytes.Buffer) error {
	return c.SerializeSqlForColumnList(out)
}

type bytesColumn struct {
	baseColumn
	isExpression
}

// Representation of VARBINARY/BLOB columns
// This function will panic if name is not valid
func BytesColumn(name string, nullable NullableColumn) NonAliasColumn {
	if !validIdentifierName(name) {
		panic("Invalid column name in bytes column")
	}
	bc := &bytesColumn{}
	bc.name = name
	bc.nullable = nullable
	return bc
}

type stringColumn struct {
	baseColumn
	isExpression
	charset   Charset
	collation Collation
}

// Representation of VARCHAR/TEXT columns
// This function will panic if name is not valid
func StrColumn(
	name string,
	charset Charset,
	collation Collation,
	nullable NullableColumn) NonAliasColumn {

	if !validIdentifierName(name) {
		panic("Invalid column name in str column")
	}
	sc := &stringColumn{charset: charset, collation: collation}
	sc.name = name
	sc.nullable = nullable
	return sc
}

type dateTimeColumn struct {
	baseColumn
	isExpression
}

// Representation of DateTime columns
// This function will panic if name is not valid
func DateTimeColumn(name string, nullable NullableColumn) NonAliasColumn {
	if !validIdentifierName(name) {
		panic("Invalid column name in datetime column")
	}
	dc := &dateTimeColumn{}
	dc.name = name
	dc.nullable = nullable
	return dc
}

type integerColumn struct {
	baseColumn
	isExpression
}

// Representation of any integer column
// This function will panic if name is not valid
func IntColumn(name string, nullable NullableColumn) NonAliasColumn {
	if !validIdentifierName(name) {
		panic("Invalid column name in int column")
	}
	ic := &integerColumn{}
	ic.name = name
	ic.nullable = nullable
	return ic
}

type doubleColumn struct {
	baseColumn
	isExpression
}

// Representation of any double column
// This function will panic if name is not valid
func DoubleColumn(name string, nullable NullableColumn) NonAliasColumn {
	if !validIdentifierName(name) {
		panic("Invalid column name in int column")
	}
	ic := &doubleColumn{}
	ic.name = name
	ic.nullable = nullable
	return ic
}

type booleanColumn struct {
	baseColumn
	isExpression

	// XXX: Maybe allow isBoolExpression (for now, not included because
	// the deferred lookup equivalent can never be isBoolExpression)
}

// Representation of TINYINT used as a bool
// This function will panic if name is not valid
func BoolColumn(name string, nullable NullableColumn) NonAliasColumn {
	if !validIdentifierName(name) {
		panic("Invalid column name in bool column")
	}
	bc := &booleanColumn{}
	bc.name = name
	bc.nullable = nullable
	return bc
}

type aliasColumn struct {
	baseColumn
	expression Expression
}

func (c *aliasColumn) SerializeSql(out *bytes.Buffer) error {
	_ = out.WriteByte('`')
	_, _ = out.WriteString(c.name)
	_ = out.WriteByte('`')
	return nil
}

func (c *aliasColumn) SerializeSqlForColumnList(out *bytes.Buffer) error {
	if !validIdentifierName(c.name) {
		return errors.Newf(
			"Invalid alias name `%s`.  Generated sql: %s",
			c.name,
			out.String())
	}
	if c.expression == nil {
		return errors.Newf(
			"Cannot alias a nil expression.  Generated sql: %s",
			out.String())
	}

	_ = out.WriteByte('(')
	if c.expression == nil {
		return errors.Newf("nil alias clause.  Generate sql: %s", out.String())
	}
	if err := c.expression.SerializeSql(out); err != nil {
		return err
	}
	_, _ = out.WriteString(") AS `")
	_, _ = out.WriteString(c.name)
	_ = out.WriteByte('`')
	return nil
}

func (c *aliasColumn) setTableName(table string) error {
	return errors.Newf(
		"Alias column '%s' should never have setTableName called on it",
		c.name)
}

// Representation of aliased clauses (expression AS name)
func Alias(name string, c Expression) Column {
	ac := &aliasColumn{}
	ac.name = name
	ac.expression = c
	return ac
}

// This is a strict subset of the actual allowed identifiers
var validIdentifierRegexp = regexp.MustCompile("^[a-zA-Z_]\\w*$")

// Returns true if the given string is suitable as an identifier.
func validIdentifierName(name string) bool {
	return validIdentifierRegexp.MatchString(name)
}

// Pseudo Column type returned by table.C(name)
type deferredLookupColumn struct {
	isProjection
	isExpression
	table   *Table
	colName string

	cachedColumn NonAliasColumn
}

func (c *deferredLookupColumn) Name() string {
	return c.colName
}

func (c *deferredLookupColumn) SerializeSqlForColumnList(
	out *bytes.Buffer) error {

	return c.SerializeSql(out)
}

func (c *deferredLookupColumn) SerializeSql(out *bytes.Buffer) error {
	if c.cachedColumn != nil {
		return c.cachedColumn.SerializeSql(out)
	}

	col, err := c.table.getColumn(c.colName)
	if err != nil {
		return err
	}

	c.cachedColumn = col
	return col.SerializeSql(out)
}

func (c *deferredLookupColumn) setTableName(table string) error {
	return errors.Newf(
		"Lookup column '%s' should never have setTableName called on it",
		c.colName)
}
