package main

import "fmt"

// See https://www.postgresql.org/docs/current/protocol-error-fields.html
type errorPostgres struct {
	severityLocalized string
	severity          string
	sqlstateCode      string
	message           string
	messageDetailed   string
	hint              string
	position          string
	positionInternal  string
	queryInternal     string
	where             string
	schemaName        string
	tableName         string
	columnName        string
	typeName          string
	constraintName    string
	file              string
	line              string
	routine           string

	additional map[byte]string
}

func (e *errorPostgres) assignField(typ byte, value string) {
	switch typ {
	case 'S':
		e.severityLocalized = value
	case 'V':
		e.severity = value
	case 'C':
		e.sqlstateCode = value
	case 'M':
		e.message = value
	case 'D':
		e.messageDetailed = value
	case 'H':
		e.hint = value
	case 'P':
		e.position = value
	case 'p':
		e.positionInternal = value
	case 'q':
		e.queryInternal = value
	case 'W':
		e.where = value
	case 's':
		e.schemaName = value
	case 't':
		e.tableName = value
	case 'c':
		e.columnName = value
	case 'd':
		e.typeName = value
	case 'n':
		e.constraintName = value
	case 'F':
		e.file = value
	case 'L':
		e.line = value
	case 'R':
		e.routine = value
	default:
		if e.additional == nil {
			e.additional = make(map[byte]string)
		}
		e.additional[typ] = value
	}
}

func (e *errorPostgres) Error() string {
	return fmt.Sprintf("%s: %s", e.severity, e.message)
}
