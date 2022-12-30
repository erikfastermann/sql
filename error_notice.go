package main

import (
	"strings"
)

// See https://www.postgresql.org/docs/current/protocol-error-fields.html
type errorAndNoticeFields struct {
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

func (e *errorAndNoticeFields) assignField(typ byte, value string) {
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

func (e *errorAndNoticeFields) String() string {
	fieldMapping := [...]struct {
		name  string
		value string
	}{
		{"Localized Severity", e.severityLocalized},
		{"Severity", e.severity},
		{"SQL State Code", e.sqlstateCode},
		{"Message", e.message},
		{"MessageDetailed", e.messageDetailed},
		{"Hint", e.hint},
		{"Position", e.position},
		{"Internal Position", e.positionInternal},
		{"Internal Query", e.queryInternal},
		{"Where", e.where},
		{"Schema Name", e.schemaName},
		{"Table Name", e.tableName},
		{"Column Name", e.columnName},
		{"Type Name", e.typeName},
		{"Constraint Name", e.constraintName},
		{"File", e.file},
		{"Line", e.line},
		{"Routine", e.routine},
	}

	var b strings.Builder
	commaNeeded := false
	for _, f := range fieldMapping {
		if f.value != "" {
			if commaNeeded {
				b.WriteString(", ")
			}
			b.WriteString(f.name)
			b.WriteString(": ")
			b.WriteString(f.value)
			commaNeeded = true
		}
	}
	return b.String()
}

type postgresError struct {
	errorAndNoticeFields
}

func (e *postgresError) Error() string {
	return e.String()
}

type notice struct {
	errorAndNoticeFields
}
