// See https://www.postgresql.org/docs/current/protocol-message-formats.html
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// TODO: maybe rename usages of kind to type

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	c, err := Connect(":5432", "erik", "unsafepassword", "data")
	if err != nil {
		return err
	}
	defer c.Close()

	if err := runConn(c); err != nil {
		return fmt.Errorf("%w (%q)", err, c.r.originalBuffer)
	}
	return nil
}

func check2[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func runConn(c *Conn) error {
	if err := c.GetQueryMetadata("select table_id, action from events where info = $1"); err != nil {
		return err
	}
	fmt.Printf("%+v\n", c.CurrentParameterOids)
	fmt.Printf("%+v\n", c.CurrentFields)

	if err := c.GetQueryMetadata("invalid query"); err != nil {
		fmt.Println(err)
	}

	start := time.Now()
	const query = "select attrelid, attnum, attname, attnotnull from pg_attribute"
	if err := c.RunQuery(query); err != nil {
		return err
	}
	for c.NextRow() {
		attrelid := check2(c.FieldInt(0))
		attnum := check2(c.FieldInt(1))
		attname := c.FieldBorrowRawBytes(2)
		attnotnull := check2(c.FieldBool(3))
		_, err := fmt.Fprintf(io.Discard, "%d - %d (notnull? %t): %s\n", attrelid, attnum, attnotnull, attname)
		if err != nil {
			return err
		}
	}
	if err := c.CloseQuery(); err != nil {
		return err
	}
	fmt.Println(time.Since(start))
	fmt.Println(c.LastCommand, c.LastRowCount)

	if err := c.RunQuery(" \n \t "); err != nil {
		fmt.Println(err)
	}

	const ddlQuery = `insert into users(id, name, password_bcrypt)
		values(nextval('serial_test_id_seq'), nextval('serial_test_id_seq'), 'unsafe')`
	if err := c.GetQueryMetadata(ddlQuery); err != nil {
		return err
	}
	if len(c.CurrentFields) != 0 {
		return errors.New("non empty row with ddl")
	}

	if err := c.Execute(ddlQuery); err != nil {
		return err
	}

	return nil
}
