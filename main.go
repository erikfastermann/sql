// See https://www.postgresql.org/docs/current/protocol-message-formats.html
package main

import (
	"fmt"
	"os"
	"time"
)

// TODO: maybe rename usages of kind to type

const (
	timeout           = 5 * time.Second
	postgresAddr      = ":5432"
	postgresUser      = "erik"
	postgresDb        = "data"
	postgresPassword  = "unsafepassword"
	postgresTestQuery = "select table_id, action from events where info = $1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	c, err := Connect()
	if err != nil {
		return err
	}
	defer c.Close()

	if err := runConn(c); err != nil {
		return fmt.Errorf("%w (%q)", err, c.r.originalBuffer)
	}
	return nil
}

func runConn(c *Conn) error {
	if err := c.GetQueryMetadata(postgresTestQuery); err != nil {
		return err
	}
	fmt.Printf("%+v\n", c.currentParameterOids)
	fmt.Printf("%+v\n", c.currentFields)

	if err := c.GetQueryMetadata("invalid query"); err != nil {
		fmt.Println(err)
	}

	start := time.Now()
	const query = "select attrelid, attnum, attname, attnotnull from pg_attribute"
	if err := c.RunQuery(query); err != nil {
		return err
	}
	fmt.Printf("\n%+v\n", c.currentFields)
	for c.NextRow() {
		for i, f := range c.currentDataFields {
			fmt.Printf("%d: null?: %t --- %q\n", i, f.isNull, f.value)
		}
	}
	if err := c.CloseQuery(); err != nil {
		return err
	}
	fmt.Println(time.Since(start))

	fmt.Println(c.lastCommand, c.lastRowCount)

	return nil
}
