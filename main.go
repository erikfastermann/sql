package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/erikfastermann/sql/postgres"
	"github.com/erikfastermann/sql/util"
)

// TODO:
//   - dependency tracking
//   - check valid go identifier
//   - no duplicate columnOption's

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("USAGE: %s config-file", os.Args[0])
	}
	configPath := os.Args[1]
	config, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	builder, err := newBuilder(config)
	if err != nil {
		return err
	}
	return builder.run()
}

// TODO: check fields set
type config struct {
	Address  string
	Username string
	Password string
	Database string

	SQLFiles []string

	// TODO: maybe as database table
	PostgresOidToGoType map[int]TypeInfo
}

type TypeInfo struct {
	Postgres string
	Go       string // package path as prefix if any
}

func loadConfig(path string) (*config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var c config
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

type builder struct {
	config *config

	conn *postgres.Conn // TODO: pool

	relations  map[int]string // oid -> relname
	attributes map[pgAttributeKey]pgAttributeValue

	parser parser
}

func newBuilder(config *config) (*builder, error) {
	conn, err := postgres.Connect(config.Address, config.Username, config.Password, config.Database)
	if err != nil {
		return nil, err
	}

	attributes, err := getPostgresAttributes(conn)
	if err != nil {
		return nil, err
	}
	relations, err := getPostgresRelations(conn)
	if err != nil {
		return nil, err
	}

	b := &builder{
		config:     config,
		conn:       conn,
		attributes: attributes,
		relations:  relations,
	}

	return b, nil
}

func getPostgresRelations(c *postgres.Conn) (oidToName map[int]string, err error) {
	const query = "select oid, relname from pg_class where reltype <> 0"
	if err := c.RunQuery(query); err != nil {
		return nil, err
	}
	relations := make(map[int]string)
	for c.NextRow() {
		oid := util.Check2(c.FieldInt(0))
		attname := string(c.FieldBorrowRawBytes(1))
		if _, ok := relations[oid]; ok {
			panic("internal error")
		}
		relations[oid] = attname
	}
	if err := c.CloseQuery(); err != nil {
		return nil, err
	}
	return relations, nil
}

type pgAttributeKey struct {
	relid, num int
}

type pgAttributeValue struct {
	name    string
	notNull bool
}

func getPostgresAttributes(c *postgres.Conn) (map[pgAttributeKey]pgAttributeValue, error) {
	const query = "select attrelid, attnum, attname, attnotnull from pg_attribute"
	if err := c.RunQuery(query); err != nil {
		return nil, err
	}
	attributes := make(map[pgAttributeKey]pgAttributeValue)
	for c.NextRow() {
		attrelid := util.Check2(c.FieldInt(0))
		attnum := util.Check2(c.FieldInt(1))
		attname := string(c.FieldBorrowRawBytes(2))
		attnotnull := util.Check2(c.FieldBool(3))

		key := pgAttributeKey{
			relid: attrelid,
			num:   attnum,
		}
		if _, ok := attributes[key]; ok {
			panic("internal error")
		}
		attributes[key] = pgAttributeValue{
			name:    attname,
			notNull: attnotnull,
		}
	}
	if err := c.CloseQuery(); err != nil {
		return nil, err
	}
	return attributes, nil
}

func (b *builder) Close() error {
	return b.conn.Close()
}

func (b *builder) run() error {
	if len(b.config.SQLFiles) == 0 {
		return errors.New("no sql files to process")
	}
	for _, sqlFile := range b.config.SQLFiles {
		if err := b.processFile(sqlFile); err != nil {
			return fmt.Errorf("%s: %w", sqlFile, err)
		}
	}
	return nil
}

func (b *builder) processFile(path string) error {
	if err := b.parser.init(path); err != nil {
		return err
	}
	if err := b.parser.buildRawDeclarations(); err != nil {
		return err
	}

	for i := range b.parser.declarations {
		decl := &b.parser.declarations[i]
		if err := b.processDeclaration(decl); err != nil {
			if errorDetail, ok := b.formatError(decl, err); ok {
				return fmt.Errorf(
					"line %d-%d: %w\n%s",
					decl.startLineIndex+1,
					decl.endLineIndex+1,
					err,
					errorDetail,
				)
			}
			return fmt.Errorf(
				"line %d-%d: %w",
				decl.startLineIndex+1,
				decl.endLineIndex+1,
				err,
			)
		}
	}

	return nil
}

func (b *builder) processDeclaration(decl *declaration) error {
	if err := decl.parse(&b.parser); err != nil {
		return err
	}

	// TODO: use row description?
	_, err := b.conn.GetQueryMetadata(decl.body)
	if err != nil {
		return err
	}

	fmt.Println(decl.String())
	fmt.Println("---")

	for _, f := range b.conn.CurrentFields {
		if f.MaybeTableOid != 0 {
			relName, ok := b.relations[f.MaybeTableOid]
			if !ok {
				panic("internal error")
			}
			key := pgAttributeKey{
				relid: f.MaybeTableOid,
				num:   f.MaybeColumnAttributeNumber,
			}
			attr, ok := b.attributes[key]
			if !ok {
				panic("internal error")
			}

			fmt.Printf("%q.%q (not null? %t)\n", relName, attr.name, attr.notNull)
		}
		fmt.Printf("%q\n", f.Name)
		typ, ok := b.config.PostgresOidToGoType[f.TypeOid]
		if !ok {
			// TODO:
			// maybe lookup oid in db for better error message
			// or create copy paste config diff
			return fmt.Errorf("unknown type oid %d", f.TypeOid)
		}
		fmt.Printf("%+v\n", typ)
		fmt.Println("---")
	}

	for _, oid := range b.conn.CurrentParameterOids {
		typ, ok := b.config.PostgresOidToGoType[oid]
		if !ok {
			// TODO:
			// maybe lookup oid in db for better error message
			// or create copy paste config diff
			return fmt.Errorf("unknown type oid %d", oid)
		}
		fmt.Printf("%+v\n", typ)
	}

	fmt.Println("-------------")

	return nil
}

func (b *builder) formatError(decl *declaration, err error) (string, bool) {
	// assumes UTF-8, does not work with characters larger than a single rune

	var postgresError *postgres.Error
	if !errors.As(err, &postgresError) {
		return "", false
	}
	if postgresError.Position == 0 {
		return "", false
	}
	remainingCharacters := postgresError.Position
	for i := decl.startLineIndex + 1; i <= decl.endLineIndex; i++ {
		line := b.parser.lineAt(i)
		remainingCharactersLine := remainingCharacters
		for range string(line) {
			remainingCharacters--
			if remainingCharacters <= 0 {
				return formatErrorLine(line, i, remainingCharactersLine-1), true
			}
		}
	}
	return "", false
}

func formatErrorLine(line []byte, lineIndex, errorPosition int) string {
	var b strings.Builder
	lineNumber := strconv.Itoa(lineIndex + 1)
	b.WriteString(lineNumber)
	b.WriteString(" | ")
	b.Write(bytes.TrimRightFunc(line, unicode.IsSpace))
	b.WriteByte('\n')
	for j := 0; j < len(lineNumber); j++ {
		b.WriteByte(' ')
	}
	b.WriteString(" | ")
	for j := 0; j < errorPosition; j++ {
		b.WriteByte(' ')
	}
	b.WriteByte('^')
	return b.String()
}
