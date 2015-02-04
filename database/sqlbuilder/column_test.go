package sqlbuilder

import (
	"bytes"
	"testing"

	gc "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	gc.TestingT(t)
}

type ColumnSuite struct {
}

var _ = gc.Suite(&ColumnSuite{})

//
// tests for baseColumn and columns that extends baseColumn
//

func (s *ColumnSuite) TestRealColumnName(c *gc.C) {
	col := IntColumn("col", Nullable)

	c.Assert(col.Name(), gc.Equals, "col")
}

func (s *ColumnSuite) TestRealColumnSerializeSqlForColumnList(c *gc.C) {
	col := IntColumn("col", Nullable)

	// Without table name
	buf := &bytes.Buffer{}

	err := col.SerializeSqlForColumnList(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`col`")

	// With table name
	err = col.setTableName("foo")
	c.Assert(err, gc.IsNil)

	buf = &bytes.Buffer{}

	err = col.SerializeSqlForColumnList(buf)
	c.Assert(err, gc.IsNil)

	sql = buf.String()
	c.Assert(sql, gc.Equals, "`foo`.`col`")
}

func (s *ColumnSuite) TestRealColumnSerializeSql(c *gc.C) {
	col := IntColumn("col", Nullable)

	// Without table name
	buf := &bytes.Buffer{}

	err := col.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`col`")

	// With table name
	err = col.setTableName("foo")
	c.Assert(err, gc.IsNil)

	buf = &bytes.Buffer{}

	err = col.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql = buf.String()
	c.Assert(sql, gc.Equals, "`foo`.`col`")
}

//
// tests for AliasCoulmns
//

func (s *ColumnSuite) TestAliasColumnName(c *gc.C) {
	col := Alias("foo", SqlFunc("max", table1Col1))

	c.Assert(col.Name(), gc.Equals, "foo")
}

func (s *ColumnSuite) TestAliasColumnSerializeSqlForColumnList(c *gc.C) {
	col := Alias("foo", SqlFunc("max", table1Col1))

	buf := &bytes.Buffer{}
	err := col.SerializeSqlForColumnList(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(err, gc.IsNil)

	c.Assert(sql, gc.Equals, "(max(`table1`.`col1`)) AS `foo`")
}

func (s *ColumnSuite) TestAliasColumnSerializeSqlForColumnListNilExpr(c *gc.C) {
	col := Alias("foo", nil)

	buf := &bytes.Buffer{}
	err := col.SerializeSqlForColumnList(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ColumnSuite) TestAliasColumnSerializeSqlForColumnListInvalidAlias(
	c *gc.C) {

	col := Alias("1234", SqlFunc("max", table1Col1))

	buf := &bytes.Buffer{}
	err := col.SerializeSqlForColumnList(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ColumnSuite) TestAliasColumnSerializeSql(c *gc.C) {
	col := Alias("foo", SqlFunc("max", table1Col1))

	buf := &bytes.Buffer{}
	err := col.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`foo`")
}

func (s *ColumnSuite) TestAliasColumnSetTableName(c *gc.C) {
	col := Alias("foo", SqlFunc("max", table1Col1))

	// should always error
	err := col.setTableName("test")
	c.Assert(err, gc.NotNil)
}

//
// tests for deferredLookkupColumnName
//

func (s *ColumnSuite) TestDeferredLookupColumnName(c *gc.C) {
	col := table1.C("foo")

	c.Assert(col.Name(), gc.Equals, "foo")
}

func (s *ColumnSuite) TestDeferredLookupColumnSerializeSqlForColumnList(
	c *gc.C) {

	col := table1.C("col1")

	buf := &bytes.Buffer{}

	err := col.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`")

	// check cached lookup
	buf = &bytes.Buffer{}

	err = col.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql = buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`")
}

func (s *ColumnSuite) TestDeferredLookupColumnSerializeSqlForColumnListInvalidName(
	c *gc.C) {
	col := table1.C("foo")

	buf := &bytes.Buffer{}

	err := col.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ColumnSuite) TestDeferredLookupColumnSerializeSql(c *gc.C) {
	col := table1.C("col1")

	buf := &bytes.Buffer{}

	err := col.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`")
}

func (s *ColumnSuite) TestDeferredLookupColumnSerializeSqlInvalidName(c *gc.C) {
	col := table1.C("foo")

	buf := &bytes.Buffer{}

	err := col.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ColumnSuite) TestDeferredLookupColumnSetTableName(c *gc.C) {
	col := table1.C("col1")

	err := col.setTableName("foo")
	c.Assert(err, gc.NotNil)
}
