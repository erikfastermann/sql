package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// TODO: maybe rename usages of kind to type

// see racy-git

const (
	timeout                   = 5 * time.Second
	readBufferSize            = 4096 * 2 * 10
	postgresAddr              = ":5432"
	postgresUser              = "erik"
	postgresDb                = "data"
	postgresPreparedStatement = "tmp" // TODO
	postgresTestQuery         = "select table_id, action from events where value = $1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	c, err := connect()
	if err != nil {
		return err
	}
	defer c.Close()

	if err := c.startup(); err != nil {
		return err
	}

	err = c.getQueryMetadata(postgresPreparedStatement, postgresTestQuery)
	if err != nil {
		return err
	}

	return nil
}

type conn struct {
	w  net.Conn // TODO: timeouts
	mw messageWriter

	r  *bufio.Reader
	mr messageReader // references the buffer of r

	processId, secretKey int

	txStatus byte
}

func connect() (*conn, error) {
	dialer := &net.Dialer{}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	netConn, err := dialer.DialContext(ctx, "tcp", postgresAddr)
	if err != nil {
		return nil, err
	}
	c := &conn{
		w: netConn,
		r: bufio.NewReaderSize(netConn, readBufferSize),
	}
	return c, nil
}

func (c *conn) Close() error {
	c.mw.reset('X')
	if err := c.mw.finalize(); err != nil {
		return err
	}
	if _, err := c.w.Write(c.mw.b); err != nil {
		return err
	}
	return c.w.Close()
}

var authenticationOk = []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}

func (c *conn) startup() error {
	if err := c.startupMessage(); err != nil {
		return err
	}
	if _, err := c.w.Write(c.mw.b); err != nil {
		return err
	}
	if err := c.readMessage(); err != nil {
		return err
	}
	if !bytes.Equal(c.mr.b, authenticationOk) {
		// TODO: implement other authentication methods
		return fmt.Errorf("expected authentication ok message, got %q", c.mr.b)
	}

	for {
		if err := c.readMessage(); err != nil {
			return err
		}
		parameter, value, err := c.parameterStatus()
		if err != nil {
			if unexpectedKind := (*errorUnexpectedKind)(nil); errors.As(err, &unexpectedKind) {
				break
			}
			return err
		}
		fmt.Printf("%q: %q\n", parameter, value)
	}

	c.mr.resetRead()
	if err := c.backendKeyData(); err != nil {
		return err
	}

	if err := c.readMessage(); err != nil {
		return err
	}
	return c.readyForQuery()
}

func (c *conn) startupMessage() error {
	c.mw.resetLengthOnly()

	const protocolVersion = 196608
	c.mw.appendInt32(protocolVersion)

	c.mw.appendString("user")
	c.mw.appendString(postgresUser)

	c.mw.appendString("database")
	c.mw.appendString(postgresDb)

	c.mw.b = append(c.mw.b, 0)

	return c.mw.finalizeLengthOnly()
}

func (c *conn) readMessage() error {
	if _, err := c.r.Discard(len(c.mr.originalBuffer)); err != nil {
		panic(err)
	}

	header, err := c.r.Peek(5)
	if err != nil {
		return fmt.Errorf("unable to read next message header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[1:])
	n, err := safeConvert[int64, int](int64(length) + 1)
	if err != nil {
		return err
	}

	b, err := c.r.Peek(n)
	if err != nil {
		return err
	}
	c.mr.b = b
	c.mr.originalBuffer = b

	errPq := c.errorMessage()
	if errPq == nil {
		c.mr.resetRead()
		return nil
	}
	return errPq
}

func (c *conn) errorMessage() error {
	if err := c.mr.expectKind('E'); err != nil {
		if unexpectedKind := (*errorUnexpectedKind)(nil); errors.As(err, &unexpectedKind) {
			return nil
		}
		return fmt.Errorf("%w (%q)", err, c.mr.originalBuffer)
	}

	if _, err := c.mr.readInt32(); err != nil {
		return fmt.Errorf("%w (%q)", err, c.mr.originalBuffer)
	}

	var errPq errorPostgres
	for {
		typ, err := c.mr.readByte()
		if err != nil {
			return fmt.Errorf("%w (%q)", err, c.mr.originalBuffer)
		}
		if typ == 0 {
			return &errPq
		}
		value, err := c.mr.readString()
		if err != nil {
			return fmt.Errorf("%w (%q)", err, c.mr.originalBuffer)
		}
		errPq.assignField(typ, string(value))
	}
}

func (c *conn) parameterStatus() (parameter []byte, value []byte, err error) {
	if err := c.mr.expectKind('S'); err != nil {
		return nil, nil, err
	}
	if _, err := c.mr.readInt32(); err != nil {
		return nil, nil, err
	}
	parameter, err = c.mr.readString()
	if err != nil {
		return nil, nil, err
	}
	value, err = c.mr.readString()
	if err != nil {
		return nil, nil, err
	}
	return parameter, value, nil
}

func (c *conn) backendKeyData() error {
	if err := c.mr.expectKind('K'); err != nil {
		return err
	}
	if _, err := c.mr.readInt32(); err != nil {
		return err
	}
	processId, err := c.mr.readInt32()
	if err != nil {
		return err
	}
	secretKey, err := c.mr.readInt32()
	if err != nil {
		return err
	}
	c.processId, c.secretKey = processId, secretKey
	return nil
}

func (c *conn) readyForQuery() error {
	if err := c.mr.expectKind('Z'); err != nil {
		return err
	}
	if _, err := c.mr.readInt32(); err != nil {
		return err
	}
	txStatus, err := c.mr.readByte()
	if err != nil {
		return err
	}
	c.txStatus = txStatus
	return nil
}

func (c *conn) getQueryMetadata(preparedStatement, query string) error {
	// TODO: cleanup needed of prepared statements?

	// TODO: should be one request
	if err := c.parse(preparedStatement, query); err != nil {
		return err
	}
	if _, err := c.w.Write(c.mw.b); err != nil {
		return err
	}
	if err := c.describeStatement(preparedStatement); err != nil {
		return err
	}
	if _, err := c.w.Write(c.mw.b); err != nil {
		return err
	}
	c.sync()
	if _, err := c.w.Write(c.mw.b); err != nil {
		return err
	}

	// ParseComplete
	if err := c.readMessage(); err != nil {
		return err
	}
	if err := c.mr.expectKind('1'); err != nil {
		return err
	}

	if err := c.readMessage(); err != nil {
		return err
	}
	oids, err := c.parameterDescription()
	if err != nil {
		return err
	}
	fmt.Println(oids)

	if err := c.readMessage(); err != nil {
		return err
	}
	fields, err := c.rowDescription()
	if err != nil {
		return err
	}
	fmt.Println(fields)

	return nil
}

func (c *conn) parse(preparedStatement, query string) error {
	c.mw.reset('P')
	c.mw.appendString(preparedStatement)
	c.mw.appendString(query)
	c.mw.appendInt16(0)
	return c.mw.finalize()
}

func (c *conn) describeStatement(preparedStatement string) error {
	c.mw.reset('D')
	c.mw.b = append(c.mw.b, 'S')
	c.mw.appendString(preparedStatement)
	return c.mw.finalize()
}

func (c *conn) sync() {
	c.mw.reset('S')
	if err := c.mw.finalize(); err != nil {
		panic(err)
	}
}

func (c *conn) parameterDescription() ([]int, error) {
	if err := c.mr.expectKind('t'); err != nil {
		return nil, err
	}
	if _, err := c.mr.readInt32(); err != nil {
		return nil, err
	}
	parameterLength, err := c.mr.readInt16()
	if err != nil {
		return nil, err
	}
	oids := make([]int, parameterLength)
	for i := 0; i < parameterLength; i++ {
		oid, err := c.mr.readInt32()
		if err != nil {
			return nil, err
		}
		oids[i] = oid
	}
	return oids, nil
}

func (c *conn) rowDescription() ([]field, error) {
	if err := c.mr.expectKind('T'); err != nil {
		return nil, err
	}
	if _, err := c.mr.readInt32(); err != nil {
		return nil, err
	}
	fieldsLength, err := c.mr.readInt16()
	if err != nil {
		return nil, err
	}

	fields := make([]field, fieldsLength)
	for i := 0; i < fieldsLength; i++ {
		name, err := c.mr.readString()
		if err != nil {
			return nil, err
		}
		maybeTableOid, err := c.mr.readInt32()
		if err != nil {
			return nil, err
		}
		maybeColumnAttributeNumber, err := c.mr.readInt16()
		if err != nil {
			return nil, err
		}
		typeOid, err := c.mr.readInt32()
		if err != nil {
			return nil, err
		}
		typeSize, err := c.mr.readInt16() // pg_type.typlen
		if err != nil {
			return nil, err
		}
		typeModifier, err := c.mr.readInt32() // pg_attribute.atttypmod
		if err != nil {
			return nil, err
		}
		// format code
		if _, err := c.mr.readInt16(); err != nil {
			return nil, err
		}

		fields[i] = field{
			name:                       string(name),
			maybeTableOid:              maybeTableOid,
			maybeColumnAttributeNumber: maybeColumnAttributeNumber,
			typeOid:                    typeOid,
			typeSize:                   typeSize,
			typeModifier:               typeModifier,
		}
	}

	return fields, nil
}

type field struct {
	name string

	maybeTableOid              int
	maybeColumnAttributeNumber int

	typeOid      int
	typeSize     int
	typeModifier int
}

// See https://www.postgresql.org/docs/current/protocol-error-fields.html
type errorPostgres struct {
	severityLocalized string
	severity          string
	sqlstateCode      string
	message           string
	messageDetailed   string
	hint              string
	position          string
	positionInternal  string
	queryInternal     string
	where             string
	schemaName        string
	tableName         string
	columnName        string
	typeName          string
	constraintName    string
	file              string
	line              string
	routine           string

	additional map[byte]string
}

func (e *errorPostgres) assignField(typ byte, value string) {
	switch typ {
	case 'S':
		e.severityLocalized = value
	case 'V':
		e.severity = value
	case 'C':
		e.sqlstateCode = value
	case 'M':
		e.message = value
	case 'D':
		e.messageDetailed = value
	case 'H':
		e.hint = value
	case 'P':
		e.position = value
	case 'p':
		e.positionInternal = value
	case 'q':
		e.queryInternal = value
	case 'W':
		e.where = value
	case 's':
		e.schemaName = value
	case 't':
		e.tableName = value
	case 'c':
		e.columnName = value
	case 'd':
		e.typeName = value
	case 'n':
		e.constraintName = value
	case 'F':
		e.file = value
	case 'L':
		e.line = value
	case 'R':
		e.routine = value
	default:
		if e.additional == nil {
			e.additional = make(map[byte]string)
		}
		e.additional[typ] = value
	}
}

func (e *errorPostgres) Error() string {
	return fmt.Sprintf("%s: %s", e.severity, e.message)
}

type errorUnexpectedKind struct {
	expected byte
	got      byte
}

func (e *errorUnexpectedKind) Error() string {
	return fmt.Sprintf(
		"unexpected message kind: expected %c, got %c",
		e.expected,
		e.got,
	)
}

type integer interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 |
		uintptr
}

var errConvert = errors.New("integer conversion failed")

func safeConvert[N, M integer](n N) (M, error) {
	if N(M(n)) != n {
		return 0, errConvert
	}
	return M(n), nil
}

type messageWriter struct {
	b          []byte
	firstError error
}

func (w *messageWriter) reset(kind byte) {
	w.firstError = nil
	w.b = w.b[:0]
	w.b = append(w.b, kind)
	w.b = append(w.b, 0, 0, 0, 0) // message length, set later
}

func (w *messageWriter) resetLengthOnly() {
	w.firstError = nil
	w.b = w.b[:0]
	w.b = append(w.b, 0, 0, 0, 0) // message length, set later
}

func (w *messageWriter) appendInt16(i int) {
	if w.firstError != nil {
		return
	}
	i16, err := safeConvert[int, int16](i)
	if err != nil {
		w.firstError = err
		return
	}
	w.b = binary.BigEndian.AppendUint16(w.b, uint16(i16))
}

func (w *messageWriter) appendInt32(i int) {
	if w.firstError != nil {
		return
	}
	i32, err := safeConvert[int, int32](i)
	if err != nil {
		w.firstError = err
		return
	}
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(i32))
}

func (w *messageWriter) appendString(s string) {
	if !w.checkContainsNoNull(s) {
		return
	}
	w.b = append(w.b, s...)
	w.b = append(w.b, 0)
}

func (w *messageWriter) appendStringBytes(b []byte) {
	if !w.checkBytesContainsNoNull(b) {
		return
	}
	w.b = append(w.b, b...)
	w.b = append(w.b, 0)
}

func (w *messageWriter) finalize() error {
	if w.firstError != nil {
		return w.firstError
	}
	l, err := safeConvert[int, uint32](len(w.b) - 1)
	if err != nil {
		w.firstError = err
		return err
	}
	binary.BigEndian.PutUint32(w.b[1:5], l)
	return nil
}

func (w *messageWriter) finalizeLengthOnly() error {
	if w.firstError != nil {
		return w.firstError
	}
	l, err := safeConvert[int, uint32](len(w.b))
	if err != nil {
		w.firstError = err
		return err
	}
	binary.BigEndian.PutUint32(w.b[:4], l)
	return nil
}

var (
	errContainsNoNullByte = errors.New("string contains no null byte")
	errContainsNullByte   = errors.New("string contains null byte")
)

func (w *messageWriter) checkContainsNoNull(s string) bool {
	if w.firstError != nil {
		return false
	}
	if strings.IndexByte(s, 0) >= 0 {
		w.firstError = errContainsNullByte
		return false
	}
	return true
}

func (w *messageWriter) checkBytesContainsNoNull(b []byte) bool {
	if w.firstError != nil {
		return false
	}
	if bytes.IndexByte(b, 0) >= 0 {
		w.firstError = errContainsNullByte
		return false
	}
	return true
}

type messageReader struct {
	b              []byte
	originalBuffer []byte
}

func (r *messageReader) resetRead() {
	r.b = r.originalBuffer
}

func (r *messageReader) expectKind(expected byte) error {
	if len(r.b) < 1 {
		return io.ErrUnexpectedEOF
	}
	got := r.b[0]
	r.b = r.b[1:]
	if got != expected {
		return &errorUnexpectedKind{
			expected: expected,
			got:      got,
		}
	}
	return nil
}

func (r *messageReader) readByte() (byte, error) {
	if len(r.b) < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	b := r.b[0]
	r.b = r.b[1:]
	return b, nil
}

func (r *messageReader) readInt16() (int, error) {
	if len(r.b) < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	u16 := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return int(int16(u16)), nil
}

func (r *messageReader) readInt32() (int, error) {
	if len(r.b) < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	u32 := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return int(int32(u32)), nil
}

func (r *messageReader) readString() ([]byte, error) {
	i, err := nullByteIndex(r.b)
	if err != nil {
		return nil, err
	}
	s := r.b[:i]
	r.b = r.b[i+1:]
	return s, nil
}

func nullByteIndex(b []byte) (int, error) {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return -1, errContainsNoNullByte
	}
	return i, nil
}
