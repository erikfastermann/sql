package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/erikfastermann/sql/postgres"
)

func check2[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var h handler
	if err := h.init("playground.sql"); err != nil {
		return err
	}
	if err := h.buildDeclarations(); err != nil {
		return err
	}
	for _, decl := range h.declarations {
		// <= is not a mistake here
		fmt.Println("--- DECL ---")
		for i := decl.startLineIndex; i <= decl.endLineIndex; i++ {
			fmt.Printf("%s", h.lineAt(i))
		}
		if err := decl.parseHeader(&h.tempBuffer); err != nil {
			return err
		}
		fmt.Printf("%#v\n", decl)
		fmt.Printf("%s, %s\n", decl.resultKind, decl.resultCount)
		fmt.Printf("func: %s\n", decl.funcName)
		fmt.Printf("struct: %s\n", decl.structName)
		fmt.Println("--------------------------")
	}

	c, err := postgres.Connect(":5432", "erik", "unsafepassword", "data")
	if err != nil {
		return err
	}
	defer c.Close()

	if _, err := c.GetQueryMetadata("select table_id, action from events where info = $1"); err != nil {
		return err
	}
	fmt.Printf("%+v\n", c.CurrentParameterOids)
	fmt.Printf("%+v\n", c.CurrentFields)

	if _, err := c.GetQueryMetadata("invalid query"); err != nil {
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
	withRowDescription, err := c.GetQueryMetadata(ddlQuery)
	if err != nil {
		return err
	}
	if withRowDescription {
		return errors.New("unexpected row description")
	}
	if len(c.CurrentFields) != 0 {
		return errors.New("non empty row with ddl")
	}

	if err := c.Execute(ddlQuery); err != nil {
		return err
	}

	const queryMultilineString = "select attrelid, attnum, attname, attnotnull from pg_attribute where attname = 'foo\nbar'"
	if err := c.RunQuery(queryMultilineString); err != nil {
		return err
	}
	if err := c.CloseQuery(); err != nil {
		return err
	}

	fmt.Println(c.GetQueryMetadata("select from pg_attribute"))
	fmt.Printf("%+v\n", c.CurrentParameterOids)
	fmt.Printf("%+v\n", c.CurrentFields)

	return nil
}
