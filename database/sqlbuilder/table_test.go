package sqlbuilder

import (
	"bytes"

	gc "gopkg.in/check.v1"
)

type TableSuite struct {
}

var _ = gc.Suite(&TableSuite{})

// NOTE: tables / columns are defined in statement_test.go

func (s *TableSuite) TestBasicColumns(c *gc.C) {
	cols := table1.Columns()

	c.Assert(len(cols), gc.Equals, 4)
	c.Assert(cols[0], gc.Equals, table1Col1)
	c.Assert(cols[1], gc.Equals, table1Col2)
	c.Assert(cols[2], gc.Equals, table1Col3)
	c.Assert(cols[3], gc.Equals, table1Col4)
}

func (s *TableSuite) TestCValidLookup(c *gc.C) {
	col := table1.C("col1")

	buf := &bytes.Buffer{}

	err := col.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`")
}

func (s *TableSuite) TestCInvalidLookup(c *gc.C) {
	col := table1.C("foo")

	buf := &bytes.Buffer{}

	err := col.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *TableSuite) TestValidForcedIndex(c *gc.C) {
	t := table1.ForceIndex("foo")
	buf := &bytes.Buffer{}
	err := t.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)
	sql := buf.String()
	c.Assert(sql, gc.Equals, "`db`.`table1` FORCE INDEX (`foo`)")

	// Ensure the original table is unchanged
	buf = &bytes.Buffer{}
	err = table1.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)
	sql = buf.String()
	c.Assert(sql, gc.Equals, "`db`.`table1`")
}

func (s *TableSuite) TestInvalidForcedIndex(c *gc.C) {
	t := table1.ForceIndex("foo\x00")
	buf := &bytes.Buffer{}
	err := t.SerializeSql("db", buf)
	c.Assert(err, gc.NotNil)
}

func (s *TableSuite) TestJoinNilLeftTable(c *gc.C) {
	join := InnerJoinOn(nil, table2, EqL(table2Col3, 123))

	buf := &bytes.Buffer{}

	err := join.SerializeSql("db", buf)
	c.Assert(err, gc.NotNil)
}

func (s *TableSuite) TestJoinNilRightTable(c *gc.C) {
	join := InnerJoinOn(table1, nil, EqL(table2Col3, 123))

	buf := &bytes.Buffer{}

	err := join.SerializeSql("db", buf)
	c.Assert(err, gc.NotNil)
}

func (s *TableSuite) TestJoinNilOnCondition(c *gc.C) {
	join := InnerJoinOn(table1, table2, nil)

	buf := &bytes.Buffer{}

	err := join.SerializeSql("db", buf)
	c.Assert(err, gc.NotNil)
}

func (s *TableSuite) TestInnerJoin(c *gc.C) {
	join := table1.InnerJoinOn(table2, Eq(table1Col3, table2Col3))

	buf := &bytes.Buffer{}

	err := join.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`db`.`table1` JOIN `db`.`table2` ON `table1`.`col3`=`table2`.`col3`")
}

func (s *TableSuite) TestLeftJoin(c *gc.C) {
	join := table1.LeftJoinOn(table2, Eq(table1Col3, table2Col3))

	buf := &bytes.Buffer{}

	err := join.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`db`.`table1` LEFT JOIN `db`.`table2` "+
			"ON `table1`.`col3`=`table2`.`col3`")
}

func (s *TableSuite) TestRightJoin(c *gc.C) {
	join := table1.RightJoinOn(table2, Eq(table1Col3, table2Col3))

	buf := &bytes.Buffer{}

	err := join.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`db`.`table1` RIGHT JOIN `db`.`table2` "+
			"ON `table1`.`col3`=`table2`.`col3`")
}

func (s *TableSuite) TestJoinColumns(c *gc.C) {
	join := table1.RightJoinOn(table2, Eq(table1Col3, table2Col3))

	cols := join.Columns()
	c.Assert(len(cols), gc.Equals, 6)
	c.Assert(cols[0], gc.Equals, table1Col1)
	c.Assert(cols[1], gc.Equals, table1Col2)
	c.Assert(cols[2], gc.Equals, table1Col3)
	c.Assert(cols[3], gc.Equals, table1Col4)
	c.Assert(cols[4], gc.Equals, table2Col3)
	c.Assert(cols[5], gc.Equals, table2Col4)
}

func (s *TableSuite) TestNestedInnerJoin(c *gc.C) {
	join1 := table1.InnerJoinOn(table2, Eq(table1Col3, table2Col3))
	join2 := join1.InnerJoinOn(table3, Eq(table1Col1, table3Col1))

	buf := &bytes.Buffer{}

	err := join2.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`db`.`table1` "+
			"JOIN `db`.`table2` ON `table1`.`col3`=`table2`.`col3` "+
			"JOIN `db`.`table3` ON `table1`.`col1`=`table3`.`col1`")
}

func (s *TableSuite) TestNestedLeftJoin(c *gc.C) {
	join1 := table1.InnerJoinOn(table2, Eq(table1Col3, table2Col3))
	join2 := join1.LeftJoinOn(table3, Eq(table1Col1, table3Col1))

	buf := &bytes.Buffer{}

	err := join2.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`db`.`table1` "+
			"JOIN `db`.`table2` ON `table1`.`col3`=`table2`.`col3` "+
			"LEFT JOIN `db`.`table3` ON `table1`.`col1`=`table3`.`col1`")
}

func (s *TableSuite) TestNestedRightJoin(c *gc.C) {
	join1 := table1.InnerJoinOn(table2, Eq(table1Col3, table2Col3))
	join2 := join1.RightJoinOn(table3, Eq(table1Col1, table3Col1))

	buf := &bytes.Buffer{}

	err := join2.SerializeSql("db", buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`db`.`table1` "+
			"JOIN `db`.`table2` ON `table1`.`col3`=`table2`.`col3` "+
			"RIGHT JOIN `db`.`table3` ON `table1`.`col1`=`table3`.`col1`")
}
