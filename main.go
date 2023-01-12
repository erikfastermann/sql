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
	postgresTestQuery = "select table_id, action from events where value = $1"
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

	if err := runConn(c); err != nil {
		return fmt.Errorf("%w (%q)", err, c.r.originalBuffer)
	}
	return nil
}

func runConn(c *conn) error {
	if err := c.startup(); err != nil {
		return err
	}

	if err := c.getQueryMetadata(postgresTestQuery); err != nil {
		return err
	}
	fmt.Printf("%+v\n", c.currentParameterOids)
	fmt.Printf("%+v\n", c.currentFields)

	start := time.Now()
	const query = "select attrelid, attnum, attname, attnotnull from pg_attribute"
	if err := c.runQuery(query); err != nil {
		return err
	}
	fmt.Printf("\n%+v\n", c.currentFields)
	for c.nextRow() {
		for i, f := range c.currentDataFields {
			fmt.Printf("%d: null?: %t --- %q\n", i, f.isNull, f.value)
		}
	}
	if err := c.finalizeQuery(); err != nil {
		return err
	}
	fmt.Println(time.Since(start))

	fmt.Println(c.lastCommand, c.lastRowCount)

	return nil
}
