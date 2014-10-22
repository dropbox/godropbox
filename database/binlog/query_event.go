package binlog

import (
	"bytes"

	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

const MaxDbsInEventMts = 254

// A representation of the query event.
//
// Query event's binlog payload is structured as follow:
//
//  Common to both 5.5 and 5.6:
//      19 bytes for common v4 event header
//      4 bytes (uint32) for thread id which executed the query
//      4 bytes (uint32) for query executation duration (in seconds)
//      1 byte (uint8) for, X, the length of database name.  Note that the
//          length does not include the null terminator character.
//      2 bytes (uint16) for error code resulting from the query execution
//      2 bytes (uint16) for, Y, the length of the variable status block
//      Y bytes for the variable status block (all status are optional):
//          flags2:
//              1 byte for Q_FLAGS2_CODE (= 0)
//              4 bytes (uint32) for flags2
//          sql mode:
//              1 byte for Q_SQL_MODE_CODE (= 1)
//              8 bytes (uint64) for sql mode
//          catalog:
//              1 byte for Q_CATALOG_NZ_CODE (= 6)
//              1 byte for length, Z
//              Z bytes for catalog data (NOTE: As of 5.6, this value should
//                  always be "std")
//          auto increment:
//              1 byte for Q_AUTO_INCREMENT (= 3)
//              2 bytes (uint16) for increment
//              2 bytes (uint16) for offset
//          charset:
//              1 byte for Q_CHARSET_CODE (= 4)
//              6 bytes for charset
//          time zone:
//              1 byte for Q_TIME_ZONE_CODE (= 5)
//              1 byte for length, R
//              R bytes for time zone
//          lc time:
//              1 byte for Q_LC_TIME_NAMES_CODE (= 7)
//              2 bytes (uint16) for lc time names number
//          charset database:
//              1 byte for Q_CHARSET_DATABASE_CODE (= 8)
//              2 bytes (uint16) fro charset database number
//          table map for update:
//              1 byte for Q_TABLE_MAP_FOR_UPDATE (= 9)
//              8 bytes (uint64) for table map for update
//          master data written: (not used by v4 events)
//              1 byte for Q_MASTER_DATA_WRITTEN (= 10)
//              4 bytes (uint32) for master data written
//          invoker:
//              1 byte for Q_INVOKER (= 11)
//              1 byte for user length, S
//              S bytes for user string
//              1 byte for host length, T
//              T bytes for host string
//          updated db name:
//              1 byte for Q_UPDATED_DB_NAMES (= 12)
//              1 byte for number of dbs, N
//              if N < MAX_DBS_IN_EVENT_MTS (= 254):
//                  N zero-terminated db name strings
//          microseconds:
//              1 byte for Q_MICROSECONDS (= 13)
//              3 bytes (uint24) for microseconds
//      X bytes for the database name (zero terminated)
//      the remaining is for the query (not zero terminated).
//  5.6 Specific:
//      (optional) 4 bytes footer for checksum.
type QueryEvent struct {
	Event

	threadId  uint32
	duration  uint32
	errorCode mysql_proto.ErrorCode_Type

	statusBytes  []byte // TODO(patrick): parse the status
	databaseName []byte
	query        []byte

	// optional status (values may be nil)
	flags2                *uint32
	sqlMode               *uint64
	catalog               []byte
	autoIncIncrement      *uint16
	autoIncOffset         *uint16
	charset               []byte
	timeZone              []byte
	lcTimeNamesNumber     *uint16
	charsetDatabaseNumber *uint16
	tableMapForUpdate     *uint64
	invokerUser           []byte
	invokerHost           []byte
	numUpdatedDbs         *uint8
	updatedDbNames        [][]byte
	microseconds          *uint32
}

// ThreadId returns the thread id which executed the query.
func (e *QueryEvent) ThreadId() uint32 {
	return e.threadId
}

// Duration returns the amount of time in second the query took to execute.
func (e *QueryEvent) Duration() uint32 {
	return e.duration
}

// ErrorCode returns the error code resulting from executing the query.
// See https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
// for additional details.
func (e *QueryEvent) ErrorCode() mysql_proto.ErrorCode_Type {
	return e.errorCode
}

// StatusBytes returns the uninterpreted status block as bytes.
func (e *QueryEvent) StatusBytes() []byte {
	return e.statusBytes
}

// DatabaseName returns the database name which was the DEFAULT database
// when the statement was executed.
func (e *QueryEvent) DatabaseName() []byte {
	return e.databaseName
}

// Query returns the query string that was executed.
func (e *QueryEvent) Query() []byte {
	return e.query
}

// Flags2 returns the flags2 status.  This returns nil if the status is not set.
func (e *QueryEvent) Flags2() *uint32 {
	return e.flags2
}

// SqlMode returns the sql mode status.  This returns nil if the status is not
// set.
func (e *QueryEvent) SqlMode() *uint64 {
	return e.sqlMode
}

// IsModeEnabled returns true iff sql mode status is set and the mode bit is
// set.
func (e *QueryEvent) IsModeEnabled(mode mysql_proto.SqlMode_BitPosition) bool {
	if e.sqlMode == nil {
		return false
	}

	return (*e.sqlMode & (uint64(1) << uint(mode))) != 0
}

// Catalog returns the catalog status.  This returns nil if the status is not
// set.
func (e *QueryEvent) Catalog() []byte {
	return e.catalog
}

// AutoIncIncrement returns the auto increment status's increment.  This
// returns nil if the status is not set.
func (e *QueryEvent) AutoIncIncrement() *uint16 {
	return e.autoIncIncrement
}

// AutoIncOffset returns the auto increment status's offset.  This returns
// nil if the status is not set.
func (e *QueryEvent) AutoIncOffset() *uint16 {
	return e.autoIncOffset
}

// Charset returns the charset status.  This returns nil if the status is not
// set.
func (e *QueryEvent) Charset() []byte {
	return e.charset
}

// TimeZone returns the time zone status.  This returns nil if the status is
// not set.
func (e *QueryEvent) TimeZone() []byte {
	return e.timeZone
}

// LcTimeNamesNumber returns the lc time names number status.  This returns nil
// if the status is not set.
func (e *QueryEvent) LcTimeNamesNumber() *uint16 {
	return e.lcTimeNamesNumber
}

// CharsetDatabaseNumber returns the charset database number status.  This
// returns nil if the status is not set.
func (e *QueryEvent) CharsetDatabaseNumber() *uint16 {
	return e.charsetDatabaseNumber
}

// TableMapForUpdate returns the table map for update id status.  This returns
// nil if the status is not set.
func (e *QueryEvent) TableMapForUpdate() *uint64 {
	return e.tableMapForUpdate
}

// InvokerUser returns the invoker status's user string.  This returns nil if
// the status is not set.
func (e *QueryEvent) InvokerUser() []byte {
	return e.invokerUser
}

// InvokerHost returns the invoker status's host string.  This returns nil if
// the status is not set.
func (e *QueryEvent) InvokerHost() []byte {
	return e.invokerHost
}

// NumUpdatedDbs returns the updated db status' size.  This return nil if the
// status is not set.
func (e *QueryEvent) NumUpdatedDbs() *uint8 {
	return e.numUpdatedDbs
}

// UpdatedDbNames returns a list of names from the updated db status.  This
// return nil if the status is not set.  Also, this returns nil when
// NumUpdatedDbs >= MaxDbsInEventMts
func (e *QueryEvent) UpdatedDbNames() [][]byte {
	return e.updatedDbNames
}

// Microseconds returns the microseconds status.  This returns nil if the
// status is not set.
func (e *QueryEvent) Microseconds() *uint32 {
	return e.microseconds
}

//
// QueryEventParser -----------------------------------------------------------
//

type QueryEventParser struct {
	hasNoTableContext
}

// QueryEventParser's EventType always returns
// mysql_proto.LogEventType_QUERY_EVENT.
func (p *QueryEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_QUERY_EVENT
}

// QueryEventParser's FixedLengthDataSize always returns 13.
func (p *QueryEventParser) FixedLengthDataSize() int {
	return 13
}

// QueryEventParser's Parse processes a raw query event into a QueryEvent.
func (p *QueryEventParser) Parse(raw *RawV4Event) (Event, error) {
	query := &QueryEvent{
		Event: raw,
	}

	type fixedBodyStruct struct {
		ThreadId           uint32
		Duration           uint32
		DatabaseNameLength uint8
		ErrorCode          uint16
		StatusLength       uint16
	}

	fixed := fixedBodyStruct{}

	_, err := readLittleEndian(raw.FixedLengthData(), &fixed)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read fixed body")
	}

	query.threadId = fixed.ThreadId
	query.duration = fixed.Duration
	query.errorCode = mysql_proto.ErrorCode_Type(fixed.ErrorCode)

	data := raw.VariableLengthData()

	dbNameEnd := int(fixed.StatusLength) + int(fixed.DatabaseNameLength)
	if dbNameEnd+1 > len(data) {
		return raw, errors.Newf("Invalid message length")
	}

	query.statusBytes = data[:fixed.StatusLength]

	query.databaseName = data[fixed.StatusLength:dbNameEnd]

	query.query = data[dbNameEnd+1:]

	err = p.parseStatus(query)
	if err != nil {
		return raw, err
	}

	return query, nil
}

func (p *QueryEventParser) parseStatus(q *QueryEvent) error {
	data := q.statusBytes
	for len(data) > 0 {
		code := data[0]
		data = data[1:]

		var err error

		// XXX(patrick): maybe check for duplicate status.
		switch mysql_proto.QueryStatusCode_Type(code) {
		case mysql_proto.QueryStatusCode_FLAGS2:
			q.flags2 = new(uint32)
			data, err = readLittleEndian(data, q.flags2)

		case mysql_proto.QueryStatusCode_SQL_MODE:
			q.sqlMode = new(uint64)
			data, err = readLittleEndian(data, q.sqlMode)

		case mysql_proto.QueryStatusCode_CATALOG:
			return errors.New("V4 events does not support catalog status")

		case mysql_proto.QueryStatusCode_AUTO_INCREMENT:
			data, err = p.parseAutoIncStatus(data, q)

		case mysql_proto.QueryStatusCode_CHARSET:
			q.charset, data, err = readSlice(data, 6)

		case mysql_proto.QueryStatusCode_TIME_ZONE:
			data, err = p.parseTimeZone(data, q)

		case mysql_proto.QueryStatusCode_CATALOG_NZ:
			data, err = p.parseCatalogNz(data, q)

		case mysql_proto.QueryStatusCode_LC_TIME_NAMES:
			q.lcTimeNamesNumber = new(uint16)
			data, err = readLittleEndian(data, q.lcTimeNamesNumber)

		case mysql_proto.QueryStatusCode_CHARSET_DATABASE:
			q.charsetDatabaseNumber = new(uint16)
			data, err = readLittleEndian(data, q.charsetDatabaseNumber)

		case mysql_proto.QueryStatusCode_TABLE_MAP_FOR_UPDATE:
			q.tableMapForUpdate = new(uint64)
			data, err = readLittleEndian(data, q.tableMapForUpdate)

		case mysql_proto.QueryStatusCode_MASTER_DATA_WRITTEN:
			return errors.New(
				"V4 events does not support master data written status")

		case mysql_proto.QueryStatusCode_INVOKER:
			data, err = p.parseInvoker(data, q)

		case mysql_proto.QueryStatusCode_UPDATED_DB_NAMES:
			data, err = p.parseUpdatedDbNames(data, q)

		case mysql_proto.QueryStatusCode_MICROSECONDS:
			data, err = p.parseMircoseconds(data, q)

		default:
			return errors.Newf("Unknown query status code: %d", int(code))
		}

		if err != nil {
			return errors.Wrapf(
				err,
				"Failed to parse query status: %s",
				mysql_proto.QueryStatusCode_Type(code).String())
		}
	}

	return nil
}

func (p *QueryEventParser) parseAutoIncStatus(data []byte, q *QueryEvent) (
	[]byte,
	error) {

	q.autoIncIncrement = new(uint16)
	data, err := readLittleEndian(data, q.autoIncIncrement)
	if err != nil {
		return data, err
	}

	q.autoIncOffset = new(uint16)
	return readLittleEndian(data, q.autoIncOffset)
}

func (p *QueryEventParser) parseTimeZone(data []byte, q *QueryEvent) (
	[]byte,
	error) {

	if len(data) == 0 {
		return data, errors.New("Not enough data")
	}

	tz, data, err := readSlice(data[1:], int(data[0]))
	q.timeZone = tz
	return data, err
}

func (p *QueryEventParser) parseCatalogNz(data []byte, q *QueryEvent) (
	[]byte,
	error) {

	if len(data) == 0 {
		return data, errors.New("Not enough data")
	}

	catalog, data, err := readSlice(data[1:], int(data[0]))
	q.catalog = catalog
	return data, err
}

func (p *QueryEventParser) parseInvoker(data []byte, q *QueryEvent) (
	[]byte,
	error) {

	if len(data) == 0 {
		return data, errors.New("Not enough data")
	}

	user, data, err := readSlice(data[1:], int(data[0]))
	q.invokerUser = user

	if len(data) == 0 {
		return data, errors.New("Not enough data")
	}

	host, data, err := readSlice(data[1:], int(data[0]))
	q.invokerHost = host
	return data, err
}

func (p *QueryEventParser) parseUpdatedDbNames(data []byte, q *QueryEvent) (
	[]byte,
	error) {

	if len(data) == 0 {
		return data, errors.New("Not enough data")
	}

	q.numUpdatedDbs = new(uint8)
	data, err := readLittleEndian(data, q.numUpdatedDbs)
	if err != nil {
		return data, err
	}

	if *q.numUpdatedDbs < MaxDbsInEventMts {
		q.updatedDbNames = make([][]byte, *q.numUpdatedDbs, *q.numUpdatedDbs)
		for i := uint8(0); i < *q.numUpdatedDbs; i++ {
			idx := bytes.IndexByte(data, byte(0))
			if idx > -1 {
				q.updatedDbNames[i] = data[:idx]
				data = data[idx+1:]
			} else {
				return data, errors.New("Not enough data")
			}
		}
	}
	return data, nil
}

func (p *QueryEventParser) parseMircoseconds(data []byte, q *QueryEvent) (
	[]byte,
	error) {

	t, data, err := readSlice(data, 3)
	if err != nil {
		return data, err
	}

	q.microseconds = new(uint32)
	*q.microseconds = LittleEndian.Uint24(t)
	return data, nil
}
