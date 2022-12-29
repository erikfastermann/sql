package main

import (
	"context"
	"errors"
	"fmt"
	"net"
)

type conn struct {
	c net.Conn // TODO: timeouts
	r *reader
	b builder

	processId, secretKey int

	txStatus byte
}

func connect() (*conn, error) {
	dialer := &net.Dialer{}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cc, err := dialer.DialContext(ctx, "tcp", postgresAddr)
	if err != nil {
		return nil, err
	}
	c := &conn{
		c: cc,
		r: newReader(cc),
	}
	return c, nil
}

func (c *conn) Close() error {
	c.b.reset()
	c.b.terminate()
	if _, err := c.c.Write(c.b.b); err != nil {
		return err
	}
	return c.c.Close()
}

func (c *conn) startup() error {
	c.b.reset()
	if err := c.b.startup(); err != nil {
		return err
	}
	if _, err := c.c.Write(c.b.b); err != nil {
		return err
	}
	if err := c.r.readMessage(); err != nil {
		return err
	}
	if err := c.r.authenticationOk(); err != nil {
		return err
	}

	for {
		if err := c.r.readMessage(); err != nil {
			return err
		}
		parameter, value, err := c.r.parameterStatus()
		if err != nil {
			if unexpectedKind := (*errorUnexpectedKind)(nil); errors.As(err, &unexpectedKind) {
				break
			}
			return err
		}
		fmt.Printf("%q: %q\n", parameter, value) // TODO
	}

	c.r.resetToOriginal()
	processId, secretKey, err := c.r.backendKeyData()
	if err != nil {
		return err
	}
	c.processId, c.secretKey = processId, secretKey

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

type metadata struct {
	parameterOids []int
	fields        []field
}

func (c *conn) getQueryMetadata(preparedStatement, query string) (*metadata, error) {
	// TODO: cleanup needed of prepared statements?

	c.b.reset()
	if err := c.b.parse(preparedStatement, query); err != nil {
		return nil, err
	}
	if err := c.b.describeStatement(preparedStatement); err != nil {
		return nil, err
	}
	c.b.sync()
	if _, err := c.c.Write(c.b.b); err != nil {
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

	m := &metadata{
		parameterOids: parameterOids,
		fields:        fields,
	}
	return m, nil
}
