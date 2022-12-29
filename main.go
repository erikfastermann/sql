// See https://www.postgresql.org/docs/current/protocol-message-formats.html
package main

import (
	"fmt"
	"os"
	"time"
)

// TODO: maybe rename usages of kind to type

const (
	timeout                   = 5 * time.Second
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

	meta, err := c.getQueryMetadata(postgresPreparedStatement, postgresTestQuery)
	if err != nil {
		return err
	}
	fmt.Printf("%+v\n", meta)

	return nil
}
