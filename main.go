package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
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
	conn net.Conn // TODO: timeouts
	b    builder

	br *bufio.Reader
	r  reader // references the buffer of r

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
		conn: netConn,
		br:   bufio.NewReaderSize(netConn, readBufferSize),
	}
	return c, nil
}

func (c *conn) Close() error {
	c.b.reset()
	c.b.terminate()
	if _, err := c.conn.Write(c.b.b); err != nil {
		return err
	}
	return c.conn.Close()
}

var authenticationOk = []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}

func (c *conn) startup() error {
	c.b.reset()
	if err := c.b.startup(); err != nil {
		return err
	}
	if _, err := c.conn.Write(c.b.b); err != nil {
		return err
	}
	if err := c.readMessage(); err != nil {
		return err
	}
	if !bytes.Equal(c.r.b, authenticationOk) {
		// TODO: implement other authentication methods
		return fmt.Errorf("expected authentication ok message, got %q", c.r.b)
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

	c.r.resetToOriginal()
	if err := c.backendKeyData(); err != nil {
		return err
	}

	if err := c.readMessage(); err != nil {
		return err
	}
	return c.readyForQuery()
}

func (c *conn) readMessage() error {
	if _, err := c.br.Discard(len(c.r.originalBuffer)); err != nil {
		panic(err)
	}

	header, err := c.br.Peek(5)
	if err != nil {
		return fmt.Errorf("unable to read next message header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[1:])
	n, err := safeConvert[int64, int](int64(length) + 1)
	if err != nil {
		return err
	}

	b, err := c.br.Peek(n)
	if err != nil {
		return err
	}
	c.r.init(b)

	errPq := c.errorMessage()
	if errPq == nil {
		c.r.resetToOriginal()
		return nil
	}
	return errPq
}

func (c *conn) errorMessage() error {
	if err := c.r.expectKind('E'); err != nil {
		if unexpectedKind := (*errorUnexpectedKind)(nil); errors.As(err, &unexpectedKind) {
			return nil
		}
		return fmt.Errorf("%w (%q)", err, c.r.originalBuffer)
	}

	if _, err := c.r.readInt32(); err != nil {
		return fmt.Errorf("%w (%q)", err, c.r.originalBuffer)
	}

	var errPq errorPostgres
	for {
		typ, err := c.r.readByte()
		if err != nil {
			return fmt.Errorf("%w (%q)", err, c.r.originalBuffer)
		}
		if typ == 0 {
			return &errPq
		}
		value, err := c.r.readString()
		if err != nil {
			return fmt.Errorf("%w (%q)", err, c.r.originalBuffer)
		}
		errPq.assignField(typ, string(value))
	}
}

func (c *conn) parameterStatus() (parameter []byte, value []byte, err error) {
	if err := c.r.expectKind('S'); err != nil {
		return nil, nil, err
	}
	if _, err := c.r.readInt32(); err != nil {
		return nil, nil, err
	}
	parameter, err = c.r.readString()
	if err != nil {
		return nil, nil, err
	}
	value, err = c.r.readString()
	if err != nil {
		return nil, nil, err
	}
	return parameter, value, nil
}

func (c *conn) backendKeyData() error {
	if err := c.r.expectKind('K'); err != nil {
		return err
	}
	if _, err := c.r.readInt32(); err != nil {
		return err
	}
	processId, err := c.r.readInt32()
	if err != nil {
		return err
	}
	secretKey, err := c.r.readInt32()
	if err != nil {
		return err
	}
	c.processId, c.secretKey = processId, secretKey
	return nil
}

func (c *conn) readyForQuery() error {
	if err := c.r.expectKind('Z'); err != nil {
		return err
	}
	if _, err := c.r.readInt32(); err != nil {
		return err
	}
	txStatus, err := c.r.readByte()
	if err != nil {
		return err
	}
	c.txStatus = txStatus
	return nil
}

func (c *conn) getQueryMetadata(preparedStatement, query string) error {
	// TODO: cleanup needed of prepared statements?

	c.b.reset()
	if err := c.b.parse(preparedStatement, query); err != nil {
		return err
	}
	if err := c.b.describeStatement(preparedStatement); err != nil {
		return err
	}
	c.b.sync()
	if _, err := c.conn.Write(c.b.b); err != nil {
		return err
	}

	// ParseComplete
	if err := c.readMessage(); err != nil {
		return err
	}
	if err := c.r.expectKind('1'); err != nil {
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

func (c *conn) parameterDescription() ([]int, error) {
	if err := c.r.expectKind('t'); err != nil {
		return nil, err
	}
	if _, err := c.r.readInt32(); err != nil {
		return nil, err
	}
	parameterLength, err := c.r.readInt16()
	if err != nil {
		return nil, err
	}
	oids := make([]int, parameterLength)
	for i := 0; i < parameterLength; i++ {
		oid, err := c.r.readInt32()
		if err != nil {
			return nil, err
		}
		oids[i] = oid
	}
	return oids, nil
}

func (c *conn) rowDescription() ([]field, error) {
	if err := c.r.expectKind('T'); err != nil {
		return nil, err
	}
	if _, err := c.r.readInt32(); err != nil {
		return nil, err
	}
	fieldsLength, err := c.r.readInt16()
	if err != nil {
		return nil, err
	}

	fields := make([]field, fieldsLength)
	for i := 0; i < fieldsLength; i++ {
		name, err := c.r.readString()
		if err != nil {
			return nil, err
		}
		maybeTableOid, err := c.r.readInt32()
		if err != nil {
			return nil, err
		}
		maybeColumnAttributeNumber, err := c.r.readInt16()
		if err != nil {
			return nil, err
		}
		typeOid, err := c.r.readInt32()
		if err != nil {
			return nil, err
		}
		typeSize, err := c.r.readInt16() // pg_type.typlen
		if err != nil {
			return nil, err
		}
		typeModifier, err := c.r.readInt32() // pg_attribute.atttypmod
		if err != nil {
			return nil, err
		}
		// format code
		if _, err := c.r.readInt16(); err != nil {
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
