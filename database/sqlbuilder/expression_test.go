package sqlbuilder

import (
	"bytes"
	"time"

	gc "gopkg.in/check.v1"
)

type ExprSuite struct {
}

var _ = gc.Suite(&ExprSuite{})

func (s *ExprSuite) TestConjunctExprEmptyList(c *gc.C) {
	expr := And()

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestConjunctExprNilInList(c *gc.C) {
	expr := And(nil, EqL(table1Col1, 1))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestConjunctExprSingleElement(c *gc.C) {
	expr := And(EqL(table1Col1, 1))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`=1")
}

func (s *ExprSuite) TestTupleExpr(c *gc.C) {

	expr := Tuple()
	buf := &bytes.Buffer{}
	err := expr.SerializeSql(buf)
	c.Assert(err, gc.NotNil)

	expr = Tuple(table1Col1, Literal(1), Literal("five"))
	err = expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"(`table1`.`col1`,1,'five')")

}

func (s *ExprSuite) TestLikeExpr(c *gc.C) {
	expr := LikeL(table1Col1, EscapeForLike("%my_prefix")+"%")

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`table1`.`col1` LIKE '\\%my\\_prefix%'")

}

func (s *ExprSuite) TestRegexExpr(c *gc.C) {
	expr := RegexpL(table1Col1, "[[:<:]]log|[[.low-line.]]log")

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`table1`.`col1` REGEXP '[[:<:]]log|[[.low-line.]]log'")

}

func (s *ExprSuite) TestAndExpr(c *gc.C) {
	expr := And(EqL(table1Col1, 1), EqL(table1Col2, 2), EqL(table1Col3, 3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"(`table1`.`col1`=1 AND `table1`.`col2`=2 AND `table1`.`col3`=3)")
}

func (s *ExprSuite) TestOrExpr(c *gc.C) {
	expr := Or(EqL(table1Col1, 1), EqL(table1Col2, 2), EqL(table1Col3, 3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"(`table1`.`col1`=1 OR `table1`.`col2`=2 OR `table1`.`col3`=3)")
}

func (s *ExprSuite) TestAddExpr(c *gc.C) {
	expr := Add(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 + 2 + 3)")
}

func (s *ExprSuite) TestSubExpr(c *gc.C) {
	expr := Sub(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 - 2 - 3)")
}

func (s *ExprSuite) TestMulExpr(c *gc.C) {
	expr := Mul(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 * 2 * 3)")
}

func (s *ExprSuite) TestDivExpr(c *gc.C) {
	expr := Div(Literal(1), Literal(2), Literal(3))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "(1 / 2 / 3)")
}

func (s *ExprSuite) TestBinaryExprNilLHS(c *gc.C) {
	expr := Gt(nil, table1Col1)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestNegateExpr(c *gc.C) {
	expr := Not(EqL(table1Col1, 123))

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "NOT (`table1`.`col1`=123)")
}

func (s *ExprSuite) TestBinaryExprNilRHS(c *gc.C) {
	expr := Lt(table1Col1, nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestEqExpr(c *gc.C) {
	expr := EqL(table1Col1, 321)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`=321")
}

func (s *ExprSuite) TestEqExprNilLHS(c *gc.C) {
	expr := EqL(table1Col1, nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` IS null")
}

func (s *ExprSuite) TestNeqExpr(c *gc.C) {
	expr := NeqL(table1Col1, 123)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`!=123")
}

func (s *ExprSuite) TestNeqExprNilLHS(c *gc.C) {
	expr := NeqL(table1Col1, nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` IS NOT null")
}

func (s *ExprSuite) TestLtExpr(c *gc.C) {
	expr := LtL(table1Col1, -1.5)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`<-1.5")
}

func (s *ExprSuite) TestLteExpr(c *gc.C) {
	expr := LteL(table1Col1, "foo\"';drop user table;")

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"`table1`.`col1`<='foo\\\"\\';drop user table;'")
}

func (s *ExprSuite) TestGtExpr(c *gc.C) {
	expr := GtL(table1Col1, 1.1)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`>1.1")
}

func (s *ExprSuite) TestGteExpr(c *gc.C) {
	expr := GteL(table1Col1, 1)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1`>=1")
}

func (s *ExprSuite) TestInExpr(c *gc.C) {
	values := []int32{1, 2, 3}
	expr := In(table1Col1, values)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` IN (1,2,3)")
}

func (s *ExprSuite) TestInExprEmptyList(c *gc.C) {
	values := []int32{}
	expr := In(table1Col1, values)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "FALSE")
}

func (s *ExprSuite) TestSqlFuncExprNilInArgList(c *gc.C) {
	expr := SqlFunc("rand", nil)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestSqlFuncExprEmptyArgList(c *gc.C) {
	expr := SqlFunc("rand")

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "rand()")
}

func (s *ExprSuite) TestSqlFuncExprNonEmptyArgList(c *gc.C) {
	expr := SqlFunc("add", table1Col1, table1Col2)

	buf := &bytes.Buffer{}

	err := expr.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "add(`table1`.`col1`,`table1`.`col2`)")
}

func (s *ExprSuite) TestOrderByClauseNilExpr(c *gc.C) {
	clause := Asc(nil)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.NotNil)
}

func (s *ExprSuite) TestAsc(c *gc.C) {
	clause := Asc(table1Col1)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` ASC")
}

func (s *ExprSuite) TestDesc(c *gc.C) {
	clause := Desc(table1Col1)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "`table1`.`col1` DESC")
}

func (s *ExprSuite) TestIf(c *gc.C) {
	test := GtL(table1Col1, 1.1)
	clause := If(test, table1Col1, table1Col2)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(
		sql,
		gc.Equals,
		"IF(`table1`.`col1`>1.1,`table1`.`col1`,`table1`.`col2`)")
}

func (s *ExprSuite) TestColumnValue(c *gc.C) {
	clause := ColumnValue(table1Col1)

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "VALUES(`table1`.`col1`)")
}

func (s *ExprSuite) TestBitwiseOr(c *gc.C) {
	clause := BitOr(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 | 2")
}

func (s *ExprSuite) TestBitwiseAnd(c *gc.C) {
	clause := BitAnd(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 & 2")
}

func (s *ExprSuite) TestBitwiseXor(c *gc.C) {
	clause := BitXor(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 ^ 2")
}

func (s *ExprSuite) TestPlus(c *gc.C) {
	clause := Plus(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 + 2")
}

func (s *ExprSuite) TestMinus(c *gc.C) {
	clause := Minus(Literal(1), Literal(2))

	buf := &bytes.Buffer{}

	err := clause.SerializeSql(buf)
	c.Assert(err, gc.IsNil)

	sql := buf.String()
	c.Assert(sql, gc.Equals, "1 - 2")
}

func (s *ExprSuite) TestInterval(c *gc.C) {
	testTable := []struct {
		interval    time.Duration
		expected    string
		expectedErr error
	}{
		{
			interval: 50 * time.Microsecond,
			expected: "INTERVAL '0:0:0:50' HOUR_MICROSECOND",
		},
		{
			interval: -50 * time.Microsecond,
			expected: "INTERVAL '-0:0:0:50' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Microsecond + 50*time.Second,
			expected: "INTERVAL '0:0:50:50' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Microsecond +
				50*time.Second +
				50*time.Minute,
			expected: "INTERVAL '0:50:50:50' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Microsecond +
				50*time.Second +
				50*time.Minute +
				50*time.Hour,
			expected: "INTERVAL '50:50:50:50' HOUR_MICROSECOND",
		},
		{
			interval: 50 * time.Hour,
			expected: "INTERVAL '50:0:0:0' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Hour + 50*time.Minute,
			expected: "INTERVAL '50:50:0:0' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Hour + 50*time.Minute + 50*time.Second,
			expected: "INTERVAL '50:50:50:0' HOUR_MICROSECOND",
		},
		{
			interval: 0,
			expected: "INTERVAL '0:0:0:0' HOUR_MICROSECOND",
		},
		{
			interval: 50 * time.Nanosecond,
			expected: "INTERVAL '0:0:0:0' HOUR_MICROSECOND",
		},
	}
	buf := &bytes.Buffer{}

	for i, tt := range testTable {
		buf.Reset()
		err := Interval(tt.interval).SerializeSql(buf)
		c.Assert(err, gc.Equals, tt.expectedErr,
			gc.Commentf("experiment #%d", i))
		if err == nil {
			c.Assert(buf.String(), gc.Equals, tt.expected,
				gc.Commentf("experiment #%d", i))
		}
	}
}
