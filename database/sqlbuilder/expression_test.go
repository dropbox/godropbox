package sqlbuilder

import (
	"bytes"

	gc "gopkg.in/check.v1"
)

type ExprSuite struct {
}

var _ = gc.Suite(&ExprSuite{})

func (s *ExprSuite) TestConjunctExprEmptyList(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := And()

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestConjunctExprNilInList(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := And(nil, EqL(table1Col1, 1))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestConjunctExprSingleElement(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := And(EqL(table1Col1, 1))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`=1")
}

func (s *ExprSuite) TestTupleExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Tuple()
	buf := &bytes.Buffer{}
	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.NotNil)

	expr = Tuple(table1Col1, Literal(1), Literal("five"))
	err = expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(`table1`.`col1`,1,'five')")
}

func (s *ExprSuite) TestLikeExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := LikeL(table1Col1, EscapeForLike("%my_prefix")+"%")

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` LIKE '\\%my\\_prefix%'")
}

func (s *ExprSuite) TestAndExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := And(EqL(table1Col1, 1), EqL(table1Col2, 2), EqL(table1Col3, 3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(`table1`.`col1`=1 AND `table1`.`col2`=2 AND `table1`.`col3`=3)")
}

func (s *ExprSuite) TestOrExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Or(EqL(table1Col1, 1), EqL(table1Col2, 2), EqL(table1Col3, 3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(`table1`.`col1`=1 OR `table1`.`col2`=2 OR `table1`.`col3`=3)")
}

func (s *ExprSuite) TestAddExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Add(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 + 2 + 3)")
}

func (s *ExprSuite) TestSubExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Sub(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 - 2 - 3)")
}

func (s *ExprSuite) TestMulExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Mul(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 * 2 * 3)")
}

func (s *ExprSuite) TestDivExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Div(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 / 2 / 3)")
}

func (s *ExprSuite) TestBinaryExprNilLHS(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Gt(nil, table1Col1)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestNegateExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Not(EqL(table1Col1, 123))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "NOT (`table1`.`col1`=123)")
}

func (s *ExprSuite) TestBinaryExprNilRHS(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := Lt(table1Col1, nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestEqExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := EqL(table1Col1, 321)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`=321")
}

func (s *ExprSuite) TestEqExprNilLHS(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := EqL(table1Col1, nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` IS null")
}

func (s *ExprSuite) TestNeqExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := NeqL(table1Col1, 123)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`!=123")
}

func (s *ExprSuite) TestNeqExprNilLHS(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := NeqL(table1Col1, nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` IS NOT null")
}

func (s *ExprSuite) TestLtExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := LtL(table1Col1, -1.5)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`<-1.5")
}

func (s *ExprSuite) TestLteExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := LteL(table1Col1, "foo\"';drop user table;")

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`table1`.`col1`<='foo\\\"\\';drop user table;'")
}

func (s *ExprSuite) TestGtExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := GtL(table1Col1, 1.1)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`>1.1")
}

func (s *ExprSuite) TestGteExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := GteL(table1Col1, 1)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`>=1")
}

func (s *ExprSuite) TestInExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	values := []int32{1, 2, 3}
	expr := In(table1Col1, values)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` IN (1,2,3)")
}

func (s *ExprSuite) TestInExprEmptyList(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	values := []int32{}
	expr := In(table1Col1, values)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "FALSE")
}

func (s *ExprSuite) TestSqlFuncExprNilInArgList(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := SqlFunc("rand", nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestSqlFuncExprEmptyArgList(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := SqlFunc("rand")

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "rand()")
}

func (s *ExprSuite) TestSqlFuncExprNonEmptyArgList(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	expr := SqlFunc("add", table1Col1, table1Col2)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "add(`table1`.`col1`,`table1`.`col2`)")
}

func (s *ExprSuite) TestOrderByClauseNilExpr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := Asc(nil)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestAsc(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := Asc(table1Col1)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` ASC")
}

func (s *ExprSuite) TestDesc(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := Desc(table1Col1)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` DESC")
}

func (s *ExprSuite) TestIf(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	test := GtL(table1Col1, 1.1)
	clause := If(test, table1Col1, table1Col2)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"IF(`table1`.`col1`>1.1,`table1`.`col1`,`table1`.`col2`)")
}

func (s *ExprSuite) TestColumnValue(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := ColumnValue(table1Col1)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "VALUES(`table1`.`col1`)")
}

func (s *ExprSuite) TestBitwiseOr(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := BitOr(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 | 2")
}

func (s *ExprSuite) TestBitwiseAnd(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := BitAnd(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 & 2")
}

func (s *ExprSuite) TestBitwiseXor(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := BitXor(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 ^ 2")
}

func (s *ExprSuite) TestPlus(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := Plus(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 + 2")
}

func (s *ExprSuite) TestMinus(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	clause := Minus(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(d, buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 - 2")
}

func (s *ExprSuite) TestBasicSubquery(c *gc.C) {
	dbName := "db"
	d := NewMySQLDialect(&dbName)

	subquery := table1.Select(table1Col2)
	outerquery := table1.Select(table1Col1, table1Col2).Where(InQ(table1Col2, Subquery(subquery)))

	sql, err := outerquery.String(d)
	c.Assert(err, gc.IsNil)

	c.Assert(sql, gc.Equals, "SELECT `table1`.`col1`,`table1`.`col2` FROM `db`.`table1` WHERE `table1`.`col2` IN (SELECT `table1`.`col2` FROM `db`.`table1`)")
}
