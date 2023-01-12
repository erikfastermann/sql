package main

import (
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

type conn struct {
	c *timeoutConn
	r *reader
	b builder

	processId, secretKey int

	txStatus byte

	parameterStatuses map[string]string

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

func connect() (*conn, error) {
	cc, err := net.DialTimeout("tcp", postgresAddr, timeout)
	if err != nil {
		return nil, err
	}
	withTimeout := &timeoutConn{
		c:       cc,
		timeout: timeout,
	}

	c := &conn{
		c:                 withTimeout,
		parameterStatuses: make(map[string]string),
	}
	c.r = newReader(c, withTimeout)
	return c, nil
}

func (c *conn) writeMessage() error {
	_, err := c.c.Write(c.b.b)
	return err
}

func (c *conn) Close() error {
	c.b.reset()
	c.b.terminate()
	if err := c.writeMessage(); err != nil {
		return err
	}
	return c.c.Close()
}

func (c *conn) startup() error {
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

func (c *conn) saslAuthScramSha256() error {
	client, err := scram.SHA256.NewClient(postgresUser, postgresPassword, "")
	if err != nil {
		return err
	}
	conv := client.NewConversation()

	initialResponse, err := conv.Step("")
	if err != nil {
		return err
	}
	println(initialResponse)
	c.b.reset()
	if err := c.b.saslInitialResponseScramSha256(initialResponse); err != nil {
		return err
	}
	if err := c.writeMessage(); err != nil {
		return err
	}

	println("A")
	if err := c.r.readMessage(); err != nil {
		return err
	}
	serverMsg, err := c.r.authenticationSASLContinue()
	if err != nil {
		return err
	}

	println(string(serverMsg))
	secondMsg, err := conv.Step(string(serverMsg))
	if err != nil {
		return err
	}
	println(secondMsg)
	c.b.reset()
	if err := c.b.saslResponse(secondMsg); err != nil {
		return err
	}
	if err := c.writeMessage(); err != nil {
		return err
	}

	println("B")
	if err := c.r.readMessage(); err != nil {
		return err
	}
	serverMsg, err = c.r.authenticationSASLFinal()
	if err != nil {
		return err
	}
	println(string(serverMsg))
	if _, err := conv.Step(string(serverMsg)); err != nil {
		return err
	}

	print("C")
	if err := c.r.readMessage(); err != nil {
		return err
	}
	_, err = c.r.authentication()
	return err
}

func (c *conn) getQueryMetadata(query string) error {
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

	if err := c.r.readMessage(); err != nil {
		return err
	}
	return c.r.readyForQuery()
}

func (c *conn) runQuery(query string) error {
	// TODO: support DDL / DML
	// TODO: recover from previous query errors (Sync) (testing required)
	// TODO: handle empty EmptyQueryResponse, probably just check the string before
	// TODO: support Extended Query (with binary and pipelining)

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

	if err := c.r.readMessage(); err != nil {
		return err
	}
	return c.r.rowDescription()
}

func (c *conn) nextRow() bool {
	if c.rowIterationDone || c.lastRowError != nil {
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

func (c *conn) finalizeQuery() error {
	if c.lastRowError != nil {
		return c.lastRowError
	}
	if !c.rowIterationDone {
		for c.nextRow() {
		}
		if c.lastRowError != nil {
			return c.lastRowError
		}
	}

	command, rows, err := c.r.commandComplete()
	if err != nil {
		return err
	}
	c.lastCommand, c.lastRowCount = command, rows

	if err := c.r.readMessage(); err != nil {
		return err
	}
	return c.r.readyForQuery()
}
