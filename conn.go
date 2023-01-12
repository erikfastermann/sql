package main

import (
	"errors"
	"net"
	"time"

	"github.com/xdg-go/scram"
)

type timeoutConn struct {
	c       net.Conn
	timeout time.Duration
}

func (c *timeoutConn) Write(p []byte) (n int, err error) {
	deadline := time.Now().Add(timeout)
	if err := c.c.SetWriteDeadline(deadline); err != nil {
		return 0, err
	}
	return c.c.Write(p)
}

func (c *timeoutConn) Read(p []byte) (n int, err error) {
	deadline := time.Now().Add(timeout)
	if err := c.c.SetReadDeadline(deadline); err != nil {
		return 0, err
	}
	return c.c.Read(p)
}

func (c *timeoutConn) Close() error {
	return c.c.Close()
}

type field struct {
	name []byte // references shared row buffer

	maybeTableOid              int
	maybeColumnAttributeNumber int

	typeOid      int
	typeSize     int
	typeModifier int

	formatCode int
}

// TODO: tx support, context support, long timeouts (for queries)
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

	currentParameterOids []int

	currentFields     []field
	currentFieldNames []byte

	// len(currentDataFields) == len(currentFields)
	currentDataFields []dataField
	rowIterationDone  bool
	lastRowError      error
	lastCommand       commandType
	lastRowCount      uint64
}

func Connect() (*Conn, error) {
	cc, err := net.DialTimeout("tcp", postgresAddr, timeout)
	if err != nil {
		return nil, err
	}
	withTimeout := &timeoutConn{
		c:       cc,
		timeout: timeout,
	}

	c := &Conn{
		c:                 withTimeout,
		parameterStatuses: make(map[string]string),
	}
	c.r = newReader(c, withTimeout)

	if err := c.startup(); err != nil {
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

func (c *Conn) startup() error {
	c.b.reset()
	if err := c.b.startup(); err != nil {
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
		if err := c.saslAuthScramSha256(); err != nil {
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

func (c *Conn) saslAuthScramSha256() error {
	client, err := scram.SHA256.NewClient(postgresUser, postgresPassword, "")
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
	if err := c.r.rowDescription(); err != nil {
		return err
	}

	return c.sync()
}

func (c *Conn) RunQuery(query string) error {
	// TODO: support DDL / DML
	// TODO: handle empty EmptyQueryResponse, probably just check the string before
	// TODO: support Extended Query (with binary and pipelining)

	if err := c.sync(); err != nil {
		return err
	}

	c.rowIterationDone = false
	c.lastRowError = nil
	c.lastCommand = commandUnknown
	c.lastRowCount = 0

	c.b.reset()
	if err := c.b.query(query); err != nil {
		return err
	}
	if err := c.writeMessage(); err != nil {
		return err
	}
	c.needSync = true

	if err := c.r.readMessage(); err != nil {
		return err
	}
	return c.r.rowDescription()
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
