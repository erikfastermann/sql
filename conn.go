package main

import (
	"net"
	"time"
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

type conn struct {
	c *timeoutConn
	r *reader
	b builder

	processId, secretKey int

	txStatus byte

	parameterStatuses map[string]string
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

	parameterStatuses := make(map[string]string)
	c := &conn{
		c:                 withTimeout,
		r:                 newReader(withTimeout, parameterStatuses),
		parameterStatuses: parameterStatuses,
	}
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

func (c *conn) readyForQuery() error {
	if err := c.r.readMessage(); err != nil {
		return err
	}
	txStatus, err := c.r.readyForQuery()
	if err != nil {
		return err
	}
	c.txStatus = txStatus
	return nil
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
	if err := c.r.authenticationOk(); err != nil {
		return err
	}

	if err := c.r.readMessage(); err != nil {
		return err
	}
	processId, secretKey, err := c.r.backendKeyData()
	if err != nil {
		return err
	}
	c.processId, c.secretKey = processId, secretKey

	return c.readyForQuery()
}

func (c *conn) getQueryMetadata(query string) (*metadata, error) {
	// TODO: cleanup needed of prepared statements?

	c.b.reset()
	if err := c.b.parse("", query); err != nil {
		return nil, err
	}
	if err := c.b.describeStatement(""); err != nil {
		return nil, err
	}
	c.b.sync()
	if err := c.writeMessage(); err != nil {
		return nil, err
	}

	if err := c.r.readMessage(); err != nil {
		return nil, err
	}
	if err := c.r.parseComplete(); err != nil {
		return nil, err
	}

	if err := c.r.readMessage(); err != nil {
		return nil, err
	}
	parameterOids, err := c.r.parameterDescription()
	if err != nil {
		return nil, err
	}

	if err := c.r.readMessage(); err != nil {
		return nil, err
	}
	fields, err := c.r.rowDescription()
	if err != nil {
		return nil, err
	}

	if err := c.readyForQuery(); err != nil {
		return nil, err
	}

	m := &metadata{
		parameterOids: parameterOids,
		fields:        fields,
	}
	return m, nil
}
