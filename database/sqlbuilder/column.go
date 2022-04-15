// Modeling of columns

package sqlbuilder

import (
	"bytes"
	"regexp"
	"sync"

	"godropbox/errors"
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
	IsPrimaryKey = true
	NotPrimaryKey = false
)

type ColumnWithIsPrimaryKey interface {
	NonAliasColumn

	// Return if the column is a primary key
	IsPrimaryKey() bool
}

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
	isPrimaryKey bool
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

func (c *baseColumn) IsPrimaryKey() bool {
	return c.isPrimaryKey
}

type bytesColumn struct {
	baseColumn
	isExpression
}

// Representation of VARBINARY/BLOB columns
// This function will panic if name is not valid
func BytesColumn(name string, nullable NullableColumn) NonAliasColumn {
	return BytesColumnWithIsPrimaryKey(name, nullable, false)
}

func BytesColumnWithIsPrimaryKey(name string, nullable NullableColumn, isPrimaryKey bool) ColumnWithIsPrimaryKey {
	if !validIdentifierName(name) {
		panic("Invalid column name in bytes column")
	}
	bc := &bytesColumn{}
	bc.name = name
	bc.nullable = nullable
	bc.isPrimaryKey = isPrimaryKey
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
	nullable NullableColumn,
) NonAliasColumn {
	return StrColumnWithIsPrimaryKey(name, charset, collation, nullable, false)
}

func StrColumnWithIsPrimaryKey(
	name string,
	charset Charset,
	collation Collation,
	nullable NullableColumn,
	isPrimaryKey bool,
) ColumnWithIsPrimaryKey {

	if !validIdentifierName(name) {
		panic("Invalid column name in str column")
	}
	sc := &stringColumn{charset: charset, collation: collation}
	sc.name = name
	sc.nullable = nullable
	sc.isPrimaryKey = isPrimaryKey
	return sc
}

type dateTimeColumn struct {
	baseColumn
	isExpression
}

// Representation of DateTime-like columns, including DATETIME, DATE, and TIMESTAMP
// This function will panic if name is not valid
func DateTimeColumn(name string, nullable NullableColumn) NonAliasColumn {
	return DateTimeColumnWithIsPrimaryKey(name, nullable, false)
}

func DateTimeColumnWithIsPrimaryKey(name string, nullable NullableColumn, isPrimaryKey bool) ColumnWithIsPrimaryKey {
	if !validIdentifierName(name) {
		panic("Invalid column name in datetime column")
	}
	dc := &dateTimeColumn{}
	dc.name = name
	dc.nullable = nullable
	dc.isPrimaryKey = isPrimaryKey
	return dc
}

type integerColumn struct {
	baseColumn
	isExpression
}

// Representation of any integer column
// This function will panic if name is not valid
func IntColumn(name string, nullable NullableColumn) NonAliasColumn {
	return IntColumnWithIsPrimaryKey(name, nullable, false)
}

func IntColumnWithIsPrimaryKey(name string, nullable NullableColumn, isPrimaryKey bool) ColumnWithIsPrimaryKey {
	if !validIdentifierName(name) {
		panic("Invalid column name in int column")
	}
	ic := &integerColumn{}
	ic.name = name
	ic.nullable = nullable
	ic.isPrimaryKey = isPrimaryKey
	return ic
}

type decimalColumn struct {
	baseColumn
	isExpression
	precision int
	scale     int
}

// Representation of DECIMAL/NUMERIC columns
// This function will panic if name is not valid
func DecimalColumn(
	name string,
	precision int,
	scale int,
	nullable NullableColumn,
) NonAliasColumn {

	return DecimalColumnWithIsPrimaryKey(name, precision, scale, nullable, false)
}

func DecimalColumnWithIsPrimaryKey(
	name string,
	precision int,
	scale int,
	nullable NullableColumn,
	isPrimaryKey bool,
) ColumnWithIsPrimaryKey {

	if !validIdentifierName(name) {
		panic("Invalid column name in decimal column")
	}
	dc := &decimalColumn{precision: precision, scale: scale}
	dc.name = name
	dc.nullable = nullable
	dc.isPrimaryKey = isPrimaryKey
	return dc
}

type doubleColumn struct {
	baseColumn
	isExpression
}

// Representation of any double column
// This function will panic if name is not valid
func DoubleColumn(name string, nullable NullableColumn) NonAliasColumn {
	return DoubleColumnWithIsPrimaryKey(name, nullable, false)
}

func DoubleColumnWithIsPrimaryKey(name string, nullable NullableColumn, isPrimaryKey bool) ColumnWithIsPrimaryKey {
	if !validIdentifierName(name) {
		panic("Invalid column name in int column")
	}
	ic := &doubleColumn{}
	ic.name = name
	ic.nullable = nullable
	ic.isPrimaryKey = isPrimaryKey
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
	return BoolColumnWithIsPrimaryKey(name, nullable, false)
}

func BoolColumnWithIsPrimaryKey(name string, nullable NullableColumn, isPrimaryKey bool) ColumnWithIsPrimaryKey {
	if !validIdentifierName(name) {
		panic("Invalid column name in bool column")
	}
	bc := &booleanColumn{}
	bc.name = name
	bc.nullable = nullable
	bc.isPrimaryKey = isPrimaryKey
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

// Holds strings as keys that have passed validation; value is nil. Used to
// avoid re-running the regex validation which was taking ~1.5% of go storage
// process cpu on panda storage leader. Expected to be low enough cardinality to
// not need cache eviction.
var identifierValidationCache sync.Map
var useIdentifierValidationCache bool

// EnableIdentifierValidationCache must be called at the start of program execution,
// strictly prior to any other sqlbuilder functions. Use only if you know that the
// cardinality of the identifiers is negligible.
func EnableIdentifierValidationCache() {
	useIdentifierValidationCache = true
}

// Returns true if the given string is suitable as an identifier.
func validIdentifierName(name string) bool {
	if !useIdentifierValidationCache {
		return validIdentifierRegexp.MatchString(name)
	}

	if _, ok := identifierValidationCache.Load(name); ok {
		return true
	}
	ok := validIdentifierRegexp.MatchString(name)
	if ok {
		identifierValidationCache.Store(name, nil)
	}
	return ok
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
