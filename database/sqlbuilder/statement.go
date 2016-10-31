package sqlbuilder

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/dropbox/godropbox/errors"
)

type Statement interface {
	// String returns generated SQL as string.
	String(database string) (sql string, err error)
}

type SelectStatement interface {
	Statement

	Where(expression BoolExpression) SelectStatement
	AndWhere(expression BoolExpression) SelectStatement
	GroupBy(expressions ...Expression) SelectStatement
	OrderBy(clauses ...OrderByClause) SelectStatement
	Limit(limit int64) SelectStatement
	Distinct() SelectStatement
	WithSharedLock() SelectStatement
	ForUpdate() SelectStatement
	Offset(offset int64) SelectStatement
	Comment(comment string) SelectStatement
	Copy() SelectStatement
}

type InsertStatement interface {
	Statement

	// Add a row of values to the insert statement.
	Add(row ...Expression) InsertStatement
	AddOnDuplicateKeyUpdate(col NonAliasColumn, expr Expression) InsertStatement
	Comment(comment string) InsertStatement
	IgnoreDuplicates(ignore bool) InsertStatement
}

// By default, rows selected by a UNION statement are out-of-order
// If you have an ORDER BY on an inner SELECT statement, the only thing
// it affects is the LIMIT clause on that inner statement (the ordering will
// still be out-of-order).
type UnionStatement interface {
	Statement

	// Warning! You cannot include table names for the next 4 clauses, or
	// you'll get errors like:
	//   Table 'server_file_journal' from one of the SELECTs cannot be used in
	//   global ORDER clause
	Where(expression BoolExpression) UnionStatement
	AndWhere(expression BoolExpression) UnionStatement
	GroupBy(expressions ...Expression) UnionStatement
	OrderBy(clauses ...OrderByClause) UnionStatement

	Limit(limit int64) UnionStatement
	Offset(offset int64) UnionStatement
}

type UpdateStatement interface {
	Statement

	Set(column NonAliasColumn, expression Expression) UpdateStatement
	Where(expression BoolExpression) UpdateStatement
	OrderBy(clauses ...OrderByClause) UpdateStatement
	Limit(limit int64) UpdateStatement
	Comment(comment string) UpdateStatement
}

type DeleteStatement interface {
	Statement

	Where(expression BoolExpression) DeleteStatement
	OrderBy(clauses ...OrderByClause) DeleteStatement
	Limit(limit int64) DeleteStatement
	Comment(comment string) DeleteStatement
}

// LockStatement is used to take Read/Write lock on tables.
// See http://dev.mysql.com/doc/refman/5.0/en/lock-tables.html
type LockStatement interface {
	Statement

	AddReadLock(table *Table) LockStatement
	AddWriteLock(table *Table) LockStatement
}

// UnlockStatement can be used to release table locks taken using LockStatement.
// NOTE: You can not selectively release a lock and continue to hold lock on
// another table. UnlockStatement releases all the lock held in the current
// session.
type UnlockStatement interface {
	Statement
}

// SetGtidNextStatement returns a SQL statement that can be used to explicitly set the next GTID.
type GtidNextStatement interface {
	Statement
}

//
// UNION SELECT Statement ======================================================
//

func Union(selects ...SelectStatement) UnionStatement {
	return &unionStatementImpl{
		selects: selects,
		limit:   -1,
		offset:  -1,
		unique:  true,
	}
}

func UnionAll(selects ...SelectStatement) UnionStatement {
	return &unionStatementImpl{
		selects: selects,
		limit:   -1,
		offset:  -1,
		unique:  false,
	}
}

// Similar to selectStatementImpl, but less complete
type unionStatementImpl struct {
	selects       []SelectStatement
	where         BoolExpression
	group         *listClause
	order         *listClause
	limit, offset int64
	// True if results of the union should be deduped.
	unique bool
}

func (us *unionStatementImpl) Where(expression BoolExpression) UnionStatement {
	us.where = expression
	return us
}

// Further filter the query, instead of replacing the filter
func (us *unionStatementImpl) AndWhere(expression BoolExpression) UnionStatement {
	if us.where == nil {
		return us.Where(expression)
	}
	us.where = And(us.where, expression)
	return us
}

func (us *unionStatementImpl) GroupBy(
	expressions ...Expression) UnionStatement {

	us.group = &listClause{
		clauses:            make([]Clause, len(expressions), len(expressions)),
		includeParentheses: false,
	}

	for i, e := range expressions {
		us.group.clauses[i] = e
	}
	return us
}

func (us *unionStatementImpl) OrderBy(
	clauses ...OrderByClause) UnionStatement {

	us.order = newOrderByListClause(clauses...)
	return us
}

func (us *unionStatementImpl) Limit(limit int64) UnionStatement {
	us.limit = limit
	return us
}

func (us *unionStatementImpl) Offset(offset int64) UnionStatement {
	us.offset = offset
	return us
}

func (us *unionStatementImpl) String(database string) (sql string, err error) {
	if len(us.selects) == 0 {
		return "", errors.Newf("Union statement must have at least one SELECT")
	}

	if len(us.selects) == 1 {
		return us.selects[0].String(database)
	}

	// Union statements in MySQL require that the same number of columns in each subquery
	var projections []Projection

	for _, statement := range us.selects {
		// do a type assertion to get at the underlying struct
		statementImpl, ok := statement.(*selectStatementImpl)
		if !ok {
			return "", errors.Newf(
				"Expected inner select statement to be of type " +
					"selectStatementImpl")
		}

		// check that for limit for statements with order by clauses
		if statementImpl.order != nil && statementImpl.limit < 0 {
			return "", errors.Newf(
				"All inner selects in Union statement must have LIMIT if " +
					"they have ORDER BY")
		}

		// check number of projections
		if projections == nil {
			projections = statementImpl.projections
		} else {
			if len(projections) != len(statementImpl.projections) {
				return "", errors.Newf(
					"All inner selects in Union statement must select the " +
						"same number of columns.  For sanity, you probably " +
						"want to select the same table columns in the same " +
						"order.  If you are selecting on multiple tables, " +
						"use Null to pad to the right number of fields.")
			}
		}
	}

	buf := new(bytes.Buffer)
	for i, statement := range us.selects {
		if i != 0 {
			if us.unique {
				_, _ = buf.WriteString(" UNION ")
			} else {
				_, _ = buf.WriteString(" UNION ALL ")
			}
		}
		_, _ = buf.WriteString("(")
		selectSql, err := statement.String(database)
		if err != nil {
			return "", err
		}
		_, _ = buf.WriteString(selectSql)
		_, _ = buf.WriteString(")")
	}

	if us.where != nil {
		_, _ = buf.WriteString(" WHERE ")
		if err = us.where.SerializeSql(buf); err != nil {
			return
		}
	}

	if us.group != nil {
		_, _ = buf.WriteString(" GROUP BY ")
		if err = us.group.SerializeSql(buf); err != nil {
			return
		}
	}

	if us.order != nil {
		_, _ = buf.WriteString(" ORDER BY ")
		if err = us.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if us.limit >= 0 {
		if us.offset >= 0 {
			_, _ = buf.WriteString(
				fmt.Sprintf(" LIMIT %d, %d", us.offset, us.limit))
		} else {
			_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d", us.limit))
		}
	}
	return buf.String(), nil
}

//
// SELECT Statement ============================================================
//

func newSelectStatement(
	table ReadableTable,
	projections []Projection) SelectStatement {

	return &selectStatementImpl{
		table:          table,
		projections:    projections,
		limit:          -1,
		offset:         -1,
		withSharedLock: false,
		forUpdate:      false,
		distinct:       false,
	}
}

// NOTE: SelectStatement purposely does not implement the Table interface since
// mysql's subquery performance is horrible.
type selectStatementImpl struct {
	table          ReadableTable
	projections    []Projection
	where          BoolExpression
	group          *listClause
	order          *listClause
	comment        string
	limit, offset  int64
	withSharedLock bool
	forUpdate      bool
	distinct       bool
}

func (s *selectStatementImpl) Copy() SelectStatement {
	ret := *s
	return &ret
}

// Further filter the query, instead of replacing the filter
func (q *selectStatementImpl) AndWhere(
	expression BoolExpression) SelectStatement {

	if q.where == nil {
		return q.Where(expression)
	}
	q.where = And(q.where, expression)
	return q
}

func (q *selectStatementImpl) Where(expression BoolExpression) SelectStatement {
	q.where = expression
	return q
}

func (q *selectStatementImpl) GroupBy(
	expressions ...Expression) SelectStatement {

	q.group = &listClause{
		clauses:            make([]Clause, len(expressions), len(expressions)),
		includeParentheses: false,
	}

	for i, e := range expressions {
		q.group.clauses[i] = e
	}
	return q
}

func (q *selectStatementImpl) OrderBy(
	clauses ...OrderByClause) SelectStatement {

	q.order = newOrderByListClause(clauses...)
	return q
}

func (q *selectStatementImpl) Limit(limit int64) SelectStatement {
	q.limit = limit
	return q
}

func (q *selectStatementImpl) Distinct() SelectStatement {
	q.distinct = true
	return q
}

func (q *selectStatementImpl) WithSharedLock() SelectStatement {
	// We don't need to grab a read lock if we're going to grab a write one
	if !q.forUpdate {
		q.withSharedLock = true
	}
	return q
}

func (q *selectStatementImpl) ForUpdate() SelectStatement {
	// Clear a request for a shared lock if we're asking for a write one
	q.withSharedLock = false
	q.forUpdate = true
	return q
}

func (q *selectStatementImpl) Offset(offset int64) SelectStatement {
	q.offset = offset
	return q
}

func (q *selectStatementImpl) Comment(comment string) SelectStatement {
	q.comment = comment
	return q
}

// Return the properly escaped SQL statement, against the specified database
func (q *selectStatementImpl) String(database string) (sql string, err error) {
	if !validIdentifierName(database) {
		return "", errors.New("Invalid database name specified")
	}

	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("SELECT ")

	if err = writeComment(q.comment, buf); err != nil {
		return
	}

	if q.distinct {
		_, _ = buf.WriteString("DISTINCT ")
	}

	if q.projections == nil || len(q.projections) == 0 {
		return "", errors.Newf(
			"No column selected.  Generated sql: %s",
			buf.String())
	}

	for i, col := range q.projections {
		if i > 0 {
			_ = buf.WriteByte(',')
		}
		if col == nil {
			return "", errors.Newf(
				"nil column selected.  Generated sql: %s",
				buf.String())
		}
		if err = col.SerializeSqlForColumnList(buf); err != nil {
			return
		}
	}

	_, _ = buf.WriteString(" FROM ")
	if q.table == nil {
		return "", errors.Newf("nil table.  Generated sql: %s", buf.String())
	}
	if err = q.table.SerializeSql(database, buf); err != nil {
		return
	}

	if q.where != nil {
		_, _ = buf.WriteString(" WHERE ")
		if err = q.where.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.group != nil {
		_, _ = buf.WriteString(" GROUP BY ")
		if err = q.group.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.order != nil {
		_, _ = buf.WriteString(" ORDER BY ")
		if err = q.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.limit >= 0 {
		if q.offset >= 0 {
			_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d, %d", q.offset, q.limit))
		} else {
			_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
		}
	}

	if q.forUpdate {
		_, _ = buf.WriteString(" FOR UPDATE")
	} else if q.withSharedLock {
		_, _ = buf.WriteString(" LOCK IN SHARE MODE")
	}

	return buf.String(), nil
}

//
// INSERT Statement ============================================================
//

func newInsertStatement(
	t WritableTable,
	columns ...NonAliasColumn) InsertStatement {

	return &insertStatementImpl{
		table:   t,
		columns: columns,
		rows:    make([][]Expression, 0, 1),
		onDuplicateKeyUpdates: make([]columnAssignment, 0, 0),
	}
}

type columnAssignment struct {
	col  NonAliasColumn
	expr Expression
}

type insertStatementImpl struct {
	table                 WritableTable
	columns               []NonAliasColumn
	rows                  [][]Expression
	onDuplicateKeyUpdates []columnAssignment
	comment               string
	ignore                bool
}

func (s *insertStatementImpl) Add(
	row ...Expression) InsertStatement {

	s.rows = append(s.rows, row)
	return s
}

func (s *insertStatementImpl) AddOnDuplicateKeyUpdate(
	col NonAliasColumn,
	expr Expression) InsertStatement {

	s.onDuplicateKeyUpdates = append(
		s.onDuplicateKeyUpdates,
		columnAssignment{col, expr})

	return s
}

func (s *insertStatementImpl) IgnoreDuplicates(ignore bool) InsertStatement {
	s.ignore = ignore
	return s
}

func (s *insertStatementImpl) Comment(comment string) InsertStatement {
	s.comment = comment
	return s
}

func (s *insertStatementImpl) String(database string) (sql string, err error) {
	if !validIdentifierName(database) {
		return "", errors.New("Invalid database name specified")
	}

	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("INSERT ")
	if s.ignore {
		_, _ = buf.WriteString("IGNORE ")
	}
	_, _ = buf.WriteString("INTO ")

	if err = writeComment(s.comment, buf); err != nil {
		return
	}

	if s.table == nil {
		return "", errors.Newf("nil table.  Generated sql: %s", buf.String())
	}

	if err = s.table.SerializeSql(database, buf); err != nil {
		return
	}

	if len(s.columns) == 0 {
		return "", errors.Newf(
			"No column specified.  Generated sql: %s",
			buf.String())
	}

	_, _ = buf.WriteString(" (")
	for i, col := range s.columns {
		if i > 0 {
			_ = buf.WriteByte(',')
		}

		if col == nil {
			return "", errors.Newf(
				"nil column in columns list.  Generated sql: %s",
				buf.String())
		}

		if err = col.SerializeSqlForColumnList(buf); err != nil {
			return
		}
	}

	if len(s.rows) == 0 {
		return "", errors.Newf(
			"No row specified.  Generated sql: %s",
			buf.String())
	}

	_, _ = buf.WriteString(") VALUES (")
	for row_i, row := range s.rows {
		if row_i > 0 {
			_, _ = buf.WriteString(", (")
		}

		if len(row) != len(s.columns) {
			return "", errors.Newf(
				"# of values does not match # of columns.  Generated sql: %s",
				buf.String())
		}

		for col_i, value := range row {
			if col_i > 0 {
				_ = buf.WriteByte(',')
			}

			if value == nil {
				return "", errors.Newf(
					"nil value in row %d col %d.  Generated sql: %s",
					row_i,
					col_i,
					buf.String())
			}

			if err = value.SerializeSql(buf); err != nil {
				return
			}
		}
		_ = buf.WriteByte(')')
	}

	if len(s.onDuplicateKeyUpdates) > 0 {
		_, _ = buf.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, colExpr := range s.onDuplicateKeyUpdates {
			if i > 0 {
				_, _ = buf.WriteString(", ")
			}

			if colExpr.col == nil {
				return "", errors.Newf(
					("nil column in on duplicate key update list.  " +
						"Generated sql: %s"),
					buf.String())
			}

			if err = colExpr.col.SerializeSqlForColumnList(buf); err != nil {
				return
			}

			_ = buf.WriteByte('=')

			if colExpr.expr == nil {
				return "", errors.Newf(
					("nil expression in on duplicate key update list.  " +
						"Generated sql: %s"),
					buf.String())
			}

			if err = colExpr.expr.SerializeSql(buf); err != nil {
				return
			}
		}
	}

	return buf.String(), nil
}

//
// UPDATE statement ===========================================================
//

func newUpdateStatement(table WritableTable) UpdateStatement {
	return &updateStatementImpl{
		table:        table,
		updateValues: make(map[NonAliasColumn]Expression),
		limit:        -1,
	}
}

type updateStatementImpl struct {
	table        WritableTable
	updateValues map[NonAliasColumn]Expression
	where        BoolExpression
	order        *listClause
	limit        int64
	comment      string
}

func (u *updateStatementImpl) Set(
	column NonAliasColumn,
	expression Expression) UpdateStatement {

	u.updateValues[column] = expression
	return u
}

func (u *updateStatementImpl) Where(expression BoolExpression) UpdateStatement {
	u.where = expression
	return u
}

func (u *updateStatementImpl) OrderBy(
	clauses ...OrderByClause) UpdateStatement {

	u.order = newOrderByListClause(clauses...)
	return u
}

func (u *updateStatementImpl) Limit(limit int64) UpdateStatement {
	u.limit = limit
	return u
}

func (u *updateStatementImpl) Comment(comment string) UpdateStatement {
	u.comment = comment
	return u
}

func (u *updateStatementImpl) String(database string) (sql string, err error) {
	if !validIdentifierName(database) {
		return "", errors.New("Invalid database name specified")
	}

	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("UPDATE ")

	if err = writeComment(u.comment, buf); err != nil {
		return
	}

	if u.table == nil {
		return "", errors.Newf("nil table.  Generated sql: %s", buf.String())
	}

	if err = u.table.SerializeSql(database, buf); err != nil {
		return
	}

	if len(u.updateValues) == 0 {
		return "", errors.Newf(
			"No column updated.  Generated sql: %s",
			buf.String())
	}

	_, _ = buf.WriteString(" SET ")
	addComma := false

	// Sorting is too hard in go, just create a second map ...
	updateValues := make(map[string]Expression)
	for col, expr := range u.updateValues {
		if col == nil {
			return "", errors.Newf(
				"nil column.  Generated sql: %s",
				buf.String())
		}

		updateValues[col.Name()] = expr
	}

	for _, col := range u.table.Columns() {
		val, inMap := updateValues[col.Name()]
		if !inMap {
			continue
		}

		if addComma {
			_, _ = buf.WriteString(", ")
		}

		if val == nil {
			return "", errors.Newf(
				"nil value.  Generated sql: %s",
				buf.String())
		}

		if err = col.SerializeSql(buf); err != nil {
			return
		}

		_ = buf.WriteByte('=')
		if err = val.SerializeSql(buf); err != nil {
			return
		}

		addComma = true
	}

	if u.where == nil {
		return "", errors.Newf(
			"Updating without a WHERE clause.  Generated sql: %s",
			buf.String())
	}

	_, _ = buf.WriteString(" WHERE ")
	if err = u.where.SerializeSql(buf); err != nil {
		return
	}

	if u.order != nil {
		_, _ = buf.WriteString(" ORDER BY ")
		if err = u.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if u.limit >= 0 {
		_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d", u.limit))
	}

	return buf.String(), nil
}

//
// DELETE statement ===========================================================
//

func newDeleteStatement(table WritableTable) DeleteStatement {
	return &deleteStatementImpl{
		table: table,
		limit: -1,
	}
}

type deleteStatementImpl struct {
	table   WritableTable
	where   BoolExpression
	order   *listClause
	limit   int64
	comment string
}

func (d *deleteStatementImpl) Where(expression BoolExpression) DeleteStatement {
	d.where = expression
	return d
}

func (d *deleteStatementImpl) OrderBy(
	clauses ...OrderByClause) DeleteStatement {

	d.order = newOrderByListClause(clauses...)
	return d
}

func (d *deleteStatementImpl) Limit(limit int64) DeleteStatement {
	d.limit = limit
	return d
}

func (d *deleteStatementImpl) Comment(comment string) DeleteStatement {
	d.comment = comment
	return d
}

func (d *deleteStatementImpl) String(database string) (sql string, err error) {
	if !validIdentifierName(database) {
		return "", errors.New("Invalid database name specified")
	}

	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("DELETE FROM ")

	if err = writeComment(d.comment, buf); err != nil {
		return
	}

	if d.table == nil {
		return "", errors.Newf("nil table.  Generated sql: %s", buf.String())
	}

	if err = d.table.SerializeSql(database, buf); err != nil {
		return
	}

	if d.where == nil {
		return "", errors.Newf(
			"Deleting without a WHERE clause.  Generated sql: %s",
			buf.String())
	}

	_, _ = buf.WriteString(" WHERE ")
	if err = d.where.SerializeSql(buf); err != nil {
		return
	}

	if d.order != nil {
		_, _ = buf.WriteString(" ORDER BY ")
		if err = d.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if d.limit >= 0 {
		_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d", d.limit))
	}

	return buf.String(), nil
}

//
// LOCK statement ===========================================================
//

// NewLockStatement returns a SQL representing empty set of locks. You need to use
// AddReadLock/AddWriteLock to add tables that need to be locked.
// NOTE: You need at least one lock in the set for it to be a valid statement.
func NewLockStatement() LockStatement {
	return &lockStatementImpl{}
}

type lockStatementImpl struct {
	locks []tableLock
}

type tableLock struct {
	t *Table
	w bool
}

// AddReadLock takes read lock on the table.
func (s *lockStatementImpl) AddReadLock(t *Table) LockStatement {
	s.locks = append(s.locks, tableLock{t: t, w: false})
	return s
}

// AddWriteLock takes write lock on the table.
func (s *lockStatementImpl) AddWriteLock(t *Table) LockStatement {
	s.locks = append(s.locks, tableLock{t: t, w: true})
	return s
}

func (s *lockStatementImpl) String(database string) (sql string, err error) {
	if !validIdentifierName(database) {
		return "", errors.New("Invalid database name specified")
	}

	if len(s.locks) == 0 {
		return "", errors.New("No locks added")
	}

	buf := new(bytes.Buffer)
	_, _ = buf.WriteString("LOCK TABLES ")

	for idx, lock := range s.locks {
		if lock.t == nil {
			return "", errors.Newf("nil table.  Generated sql: %s", buf.String())
		}

		if err = lock.t.SerializeSql(database, buf); err != nil {
			return
		}

		if lock.w {
			_, _ = buf.WriteString(" WRITE")
		} else {
			_, _ = buf.WriteString(" READ")
		}

		if idx != len(s.locks)-1 {
			_, _ = buf.WriteString(", ")
		}
	}

	return buf.String(), nil
}

// NewUnlockStatement returns SQL statement that can be used to release table locks
// grabbed by the current session.
func NewUnlockStatement() UnlockStatement {
	return &unlockStatementImpl{}
}

type unlockStatementImpl struct {
}

func (s *unlockStatementImpl) String(database string) (sql string, err error) {
	return "UNLOCK TABLES", nil
}

// Set GTID_NEXT statment returns a SQL statment that can be used to explicitly set the next GTID.
func NewGtidNextStatment(sid []byte, gno uint64) GtidNextStatement {
	return &gtidNextStatementImpl{
		sid: sid,
		gno: gno,
	}
}

type gtidNextStatementImpl struct {
	sid []byte
	gno uint64
}

func (s *gtidNextStatementImpl) String(database string) (sql string, err error) {
	// This statement sets a session local variable defining what the next transaction ID is.  It
	// does not interact with other MySQL sessions. It is neither a DDL nor DML statement, so we
	// don't have to worry about data corruption.
	// Because of the string formatting (hex plus an integer), can't morph into another statement.
	// See: https://dev.mysql.com/doc/refman/5.7/en/replication-options-gtids.html
	const gtidFormatString = "SET GTID_NEXT=\"%x-%x-%x-%x-%x:%d\""

	buf := new(bytes.Buffer)
	_, _ = buf.WriteString(fmt.Sprintf(gtidFormatString,
		s.sid[:4], s.sid[4:6], s.sid[6:8], s.sid[8:10], s.sid[10:], s.gno))
	return buf.String(), nil
}

//
// Util functions =============================================================
//

// Once again, teisenberger is lazy.  Here's a quick filter on comments
var validCommentRegexp *regexp.Regexp = regexp.MustCompile("^[\\w .?]*$")

func isValidComment(comment string) bool {
	return validCommentRegexp.MatchString(comment)
}

func writeComment(comment string, buf *bytes.Buffer) error {
	if comment != "" {
		_, _ = buf.WriteString("/* ")
		if !isValidComment(comment) {
			return errors.Newf("Invalid comment: %s", comment)
		}
		_, _ = buf.WriteString(comment)
		_, _ = buf.WriteString(" */")
	}
	return nil
}

func newOrderByListClause(clauses ...OrderByClause) *listClause {
	ret := &listClause{
		clauses:            make([]Clause, len(clauses), len(clauses)),
		includeParentheses: false,
	}

	for i, c := range clauses {
		ret.clauses[i] = c
	}

	return ret
}
