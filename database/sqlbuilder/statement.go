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
	GroupBy(expressions ...Expression) SelectStatement
	OrderBy(clauses ...OrderByClause) SelectStatement
	Limit(limit int64) SelectStatement
	WithSharedLock() SelectStatement
	ForUpdate() SelectStatement
	Offset(offset int64) SelectStatement
	Comment(comment string) SelectStatement
}

type InsertStatement interface {
	Statement

	// Add a row of values to the insert statement.
	Add(row ...Expression) InsertStatement
	AddOnDuplicateKeyUpdate(col NonAliasColumn, expr Expression) InsertStatement
	Comment(comment string) InsertStatement
	IgnoreDuplicates(ignore bool) InsertStatement
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
// NOTE: You can not selectively release a lock and continue to hold lock on another
// table. UnlockStatement releases all the lock held in the current session.
type UnlockStatement interface {
	Statement
}

//
// UNION SELECT Statement ======================================================
//

func Union(selects ...SelectStatement) Statement {
	return &unionStatementImpl{
		selects: selects,
	}
}

type unionStatementImpl struct {
	selects []SelectStatement
}

func (s *unionStatementImpl) String(database string) (sql string, err error) {
	// XXX(teisenbe): Once sqlproxy gets fixed, re-parenthesis the UNION selects
	// We don't have any use cases where they matter, but if one were to use limits/order by with
	// selects and unions, it could be problematic
	if len(s.selects) == 0 {
		return "", errors.Newf("Union statement must have at least one SELECT")
	}
	if len(s.selects) == 1 {
		return s.selects[0].String(database)
	}

	buf := new(bytes.Buffer)
	for i, statement := range s.selects {
		if i != 0 {
			buf.WriteString(" UNION ")
		}
		//buf.WriteString("(")
		selectSql, err := statement.String(database)
		if err != nil {
			return "", err
		}
		buf.WriteString(selectSql)
		//buf.WriteString(")")
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
	buf.WriteString("SELECT ")

	if err = writeComment(q.comment, buf); err != nil {
		return
	}

	if q.projections == nil || len(q.projections) == 0 {
		return "", errors.Newf(
			"No column selected.  Generated sql: %s",
			buf.String())
	}

	for i, col := range q.projections {
		if i > 0 {
			buf.WriteByte(',')
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

	buf.WriteString(" FROM ")
	if q.table == nil {
		return "", errors.Newf("nil table.  Generated sql: %s", buf.String())
	}
	if err = q.table.SerializeSql(database, buf); err != nil {
		return
	}

	if q.where != nil {
		buf.WriteString(" WHERE ")
		if err = q.where.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.group != nil {
		buf.WriteString(" GROUP BY ")
		if err = q.group.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.order != nil {
		buf.WriteString(" ORDER BY ")
		if err = q.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if q.limit >= 0 {
		if q.offset >= 0 {
			buf.WriteString(fmt.Sprintf(" LIMIT %d, %d", q.offset, q.limit))
		} else {
			buf.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
		}
	}

	if q.forUpdate {
		buf.WriteString(" FOR UPDATE")
	} else if q.withSharedLock {
		buf.WriteString(" LOCK IN SHARE MODE")
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
	buf.WriteString("INSERT ")
	if s.ignore {
		buf.WriteString("IGNORE ")
	}
	buf.WriteString("INTO ")

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

	buf.WriteString(" (")
	for i, col := range s.columns {
		if i > 0 {
			buf.WriteByte(',')
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

	buf.WriteString(") VALUES (")
	for row_i, row := range s.rows {
		if row_i > 0 {
			buf.WriteString(", (")
		}

		if len(row) != len(s.columns) {
			return "", errors.Newf(
				"# of values does not match # of columns.  Generated sql: %s",
				buf.String())
		}

		for col_i, value := range row {
			if col_i > 0 {
				buf.WriteByte(',')
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
		buf.WriteByte(')')
	}

	if len(s.onDuplicateKeyUpdates) > 0 {
		buf.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, colExpr := range s.onDuplicateKeyUpdates {
			if i > 0 {
				buf.WriteString(", ")
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

			buf.WriteByte('=')

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
	buf.WriteString("UPDATE ")

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

	buf.WriteString(" SET ")
	addComma := false
	for col, val := range u.updateValues {
		if addComma {
			buf.WriteString(", ")
		}

		if col == nil {
			return "", errors.Newf(
				"nil column.  Generated sql: %s",
				buf.String())
		}

		if val == nil {
			return "", errors.Newf(
				"nil value.  Generated sql: %s",
				buf.String())
		}

		if err = col.SerializeSql(buf); err != nil {
			return
		}

		buf.WriteByte('=')
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

	buf.WriteString(" WHERE ")
	if err = u.where.SerializeSql(buf); err != nil {
		return
	}

	if u.order != nil {
		buf.WriteString(" ORDER BY ")
		if err = u.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if u.limit >= 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", u.limit))
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
	buf.WriteString("DELETE FROM ")

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

	buf.WriteString(" WHERE ")
	if err = d.where.SerializeSql(buf); err != nil {
		return
	}

	if d.order != nil {
		buf.WriteString(" ORDER BY ")
		if err = d.order.SerializeSql(buf); err != nil {
			return
		}
	}

	if d.limit >= 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", d.limit))
	}

	return buf.String(), nil
}

//
// LOCK statement ===========================================================
//

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

func (s *lockStatementImpl) AddReadLock(t *Table) LockStatement {
	s.locks = append(s.locks, tableLock{t: t, w: false})
	return s
}

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
	buf.WriteString("LOCK TABLES ")

	for idx, lock := range s.locks {
		if lock.t == nil {
			return "", errors.Newf("nil table.  Generated sql: %s", buf.String())
		}

		if err = lock.t.SerializeSql(database, buf); err != nil {
			return
		}

		if lock.w {
			buf.WriteString(" WRITE")
		} else {
			buf.WriteString(" READ")
		}

		if idx != len(s.locks)-1 {
			buf.WriteString(", ")
		}
	}

	return buf.String(), nil
}

func NewUnlockStatement() UnlockStatement {
	return &unlockStatementImpl{}
}

type unlockStatementImpl struct {
}

func (s *unlockStatementImpl) String(database string) (sql string, err error) {
	return "UNLOCK TABLES", nil
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
		buf.WriteString("/* ")
		if !isValidComment(comment) {
			return errors.Newf("Invalid comment: %s", comment)
		}
		buf.WriteString(comment)
		buf.WriteString(" */")
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
