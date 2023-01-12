// See https://www.postgresql.org/docs/current/protocol-message-formats.html
package postgres

import (
	"errors"
	"net"
	"strings"
	"time"

	"github.com/xdg-go/scram"
)

// TODO: maybe rename usages of kind to type

type timeoutConn struct {
	c       net.Conn
	timeout time.Duration
}

const connTimeout = 5 * time.Second

func (c *timeoutConn) Write(p []byte) (n int, err error) {
	deadline := time.Now().Add(c.timeout)
	if err := c.c.SetWriteDeadline(deadline); err != nil {
		return 0, err
	}
	return c.c.Write(p)
}

func (c *timeoutConn) Read(p []byte) (n int, err error) {
	deadline := time.Now().Add(c.timeout)
	if err := c.c.SetReadDeadline(deadline); err != nil {
		return 0, err
	}
	return c.c.Read(p)
}

func (c *timeoutConn) Close() error {
	return c.c.Close()
}

type Field struct {
	Name []byte // references shared row buffer

	MaybeTableOid              int
	MaybeColumnAttributeNumber int

	TypeOid      int
	TypeSize     int
	TypeModifier int

	FormatCode int
}

// TODO: tx support, context support (with query cancellation),
// long timeouts (for queries), Extended Query (with binary and pipelining)
type Conn struct {
	c *timeoutConn
	r *reader
	b builder

	txStatus byte
	// set by public methods after first write
	needSync bool
	// set when closed or if an error occurs when syncing (except postgres errors)
	fatalError error

	processId, secretKey int
	parameterStatuses    map[string]string

	CurrentParameterOids []int

	CurrentFields     []Field
	currentFieldNames []byte

	// len(currentDataFields) == len(currentFields)
	currentDataFields []dataField
	rowIterationDone  bool
	lastRowError      error
	LastCommand       CommandType
	LastRowCount      int64
}

func Connect(addr, username, password, db string) (*Conn, error) {
	cc, err := net.DialTimeout("tcp", addr, connTimeout)
	if err != nil {
		return nil, err
	}
	withTimeout := &timeoutConn{
		c:       cc,
		timeout: connTimeout,
	}

	c := &Conn{
		c:                 withTimeout,
		parameterStatuses: make(map[string]string),
	}
	c.r = newReader(c, withTimeout)

	if err := c.startup(username, password, db); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) writeMessage() error {
	_, err := c.c.Write(c.b.b)
	return err
}

var errConnClosed = errors.New("connection closed")

func (c *Conn) Close() error {
	if c.fatalError != nil {
		return c.fatalError
	}
	c.fatalError = errConnClosed

	c.b.reset()
	c.b.terminate()
	writeErr := c.writeMessage()
	closeErr := c.c.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (c *Conn) startup(username, password, db string) error {
	c.b.reset()
	if err := c.b.startup(username, db); err != nil {
		return err
	}
	if err := c.writeMessage(); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	saslAuth, err := c.r.authentication()
	if err != nil {
		return err
	}
	switch saslAuth {
	case saslAuthMechanismNone:
		// AuthenticationOk
	case saslAuthMechanismScramSha256:
		if err := c.saslAuthScramSha256(username, password); err != nil {
			return err
		}
	default:
		panic("unreachable")
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	if err := c.r.backendKeyData(); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	return c.r.readyForQuery()
}

func (c *Conn) saslAuthScramSha256(username, password string) error {
	client, err := scram.SHA256.NewClient(username, password, "")
	if err != nil {
		return err
	}
	conv := client.NewConversation()

	initialResponse, err := conv.Step("")
	if err != nil {
		return err
	}
	c.b.reset()
	if err := c.b.saslInitialResponseScramSha256(initialResponse); err != nil {
		return err
	}
	if err := c.writeMessage(); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	serverMsg, err := c.r.authenticationSASLContinue()
	if err != nil {
		return err
	}

	secondMsg, err := conv.Step(string(serverMsg))
	if err != nil {
		return err
	}
	c.b.reset()
	if err := c.b.saslResponse(secondMsg); err != nil {
		return err
	}
	if err := c.writeMessage(); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	serverMsg, err = c.r.authenticationSASLFinal()
	if err != nil {
		return err
	}
	if _, err := conv.Step(string(serverMsg)); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	_, err = c.r.authentication()
	return err
}

func (c *Conn) sync() error {
	if c.fatalError != nil {
		return c.fatalError
	}
	if !c.needSync {
		return nil
	}
	for {
		if err := c.consumeSync(); err != nil {
			if pqErr := (*postgresError)(nil); errors.As(err, &pqErr) {
				// ignore postgres errors when syncing
				continue
			}
			_ = c.Close()
			c.fatalError = err
			return err
		}
		c.needSync = false
		return nil
	}
}

func (c *Conn) consumeSync() error {
	for {
		if err := c.r.readMessage(); err != nil {
			return err
		}
		kind, err := c.r.peekKind()
		if err != nil {
			return err
		}
		if kind != 'Z' {
			continue
		}
		if err := c.r.readyForQuery(); err != nil {
			return err
		}
		return nil
	}
}

func (c *Conn) GetQueryMetadata(query string) error {
	if err := c.sync(); err != nil {
		return err
	}

	c.b.reset()
	if err := c.b.parse("", query); err != nil {
		return err
	}
	if err := c.b.describeStatement(""); err != nil {
		return err
	}
	c.b.sync()
	if err := c.writeMessage(); err != nil {
		return err
	}
	c.needSync = true

	if err := c.r.readMessage(); err != nil {
		return err
	}
	if err := c.r.parseComplete(); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	if err := c.r.parameterDescription(); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	kind, err := c.r.peekKind()
	if err != nil {
		return err
	}
	if kind == 'n' {
		if err := c.r.noData(); err != nil {
			return err
		}
		c.CurrentFields = c.CurrentFields[:0]
	} else {
		if err := c.r.rowDescription(); err != nil {
			return err
		}
	}

	return c.sync()
}

func (c *Conn) Execute(query string) error {
	if err := c.queryBase(query); err != nil {
		return err
	}
	if err := c.r.readMessage(); err != nil {
		return err
	}
	if err := c.r.commandComplete(); err != nil {
		return err
	}
	return c.sync()
}

func (c *Conn) RunQuery(query string) error {
	if err := c.queryBase(query); err != nil {
		return err
	}
	if err := c.r.readMessage(); err != nil {
		return err
	}
	return c.r.rowDescription() // text format is currently assumed
}

var errBlankQueryString = errors.New("blank query string")

func (c *Conn) queryBase(query string) error {
	if err := c.sync(); err != nil {
		return err
	}

	c.rowIterationDone = false
	c.lastRowError = nil
	c.LastCommand = CommandUnknown
	c.LastRowCount = 0

	if strings.TrimSpace(query) == "" {
		return errBlankQueryString
	}

	c.b.reset()
	if err := c.b.query(query); err != nil {
		return err
	}
	if err := c.writeMessage(); err != nil {
		return err
	}
	c.needSync = true
	return nil
}

func (c *Conn) NextRow() bool {
	if c.fatalError != nil || c.rowIterationDone || c.lastRowError != nil {
		return false
	}

	if err := c.r.readMessage(); err != nil {
		c.lastRowError = err
		return false
	}
	kind, err := c.r.peekKind()
	if err != nil {
		c.lastRowError = err
		return false
	}
	if kind == 'C' {
		c.rowIterationDone = true
		return false
	}
	if err := c.r.dataRow(); err != nil {
		c.lastRowError = err
		return false
	}
	return true
}

var (
	errInvalidResultRowIndex = errors.New("invalid result row index")
	errInvalidColumnType     = errors.New("invalid column type")
	ErrNullValue             = errors.New("null value")
)

func (c *Conn) FieldsLength() int {
	return len(c.CurrentFields)
}

func (c *Conn) FieldIsNull(index int) bool {
	if index < 0 || index >= len(c.currentDataFields) {
		panic(errInvalidResultRowIndex)
	}
	return c.currentDataFields[index].isNull
}

func (c *Conn) FieldBorrowRawBytes(index int) []byte {
	if index < 0 || index >= len(c.CurrentFields) {
		panic(errInvalidResultRowIndex)
	}
	if c.currentDataFields[index].isNull {
		panic(ErrNullValue)
	}
	return c.currentDataFields[index].value
}

func (c *Conn) FieldInt(index int) (int, error) {
	if index < 0 || index >= len(c.CurrentFields) {
		panic(errInvalidResultRowIndex)
	}
	if c.currentDataFields[index].isNull {
		panic(ErrNullValue)
	}
	i64, err := parseInt64(c.currentDataFields[index].value)
	if err != nil {
		return -1, err
	}
	return safeConvert[int64, int](i64)
}

func (c *Conn) FieldBool(index int) (bool, error) {
	if index < 0 || index >= len(c.CurrentFields) {
		panic(errInvalidResultRowIndex)
	}
	if c.currentDataFields[index].isNull {
		panic(ErrNullValue)
	}

	value := c.currentDataFields[index].value
	switch string(value) { // does not allocate
	case "f":
		return false, nil
	case "t":
		return true, nil
	default:
		return false, errInvalidColumnType
	}
}

func (c *Conn) CloseQuery() error {
	if c.lastRowError != nil {
		return c.lastRowError
	}
	if !c.rowIterationDone {
		for c.NextRow() {
		}
		if c.lastRowError != nil {
			return c.lastRowError
		}
	}

	if err := c.r.commandComplete(); err != nil {
		return err
	}
	return c.sync()
}
