package postgres

import "strings"

type additionalErrorAndNoticeField struct {
	identifier byte
	value      string
}

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

	additional []additionalErrorAndNoticeField
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
		e.additional = append(e.additional, additionalErrorAndNoticeField{
			identifier: typ,
			value:      value,
		})
	}
}

func (e *errorAndNoticeFields) String() string {
	var w twoPassWriter
	e.writeTo(&w)
	w.b.Grow(w.l)
	e.writeTo(&w)
	return w.b.String()
}

func (e *errorAndNoticeFields) writeTo(w *twoPassWriter) {
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

	commaNeeded := false

	for _, f := range fieldMapping {
		if f.value != "" {
			if commaNeeded {
				w.writeString(", ")
			}
			w.writeString(f.name)
			w.writeString(": ")
			w.writeString(f.value)
			commaNeeded = true
		}
	}

	for _, f := range e.additional {
		if commaNeeded {
			w.writeString(", ")
		}
		w.writeByte(f.identifier)
		w.writeString(": ")
		w.writeString(f.value)
		commaNeeded = true
	}
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

type twoPassWriter struct {
	l int
	b strings.Builder
}

func (w *twoPassWriter) writeString(s string) {
	if w.b.Cap() != 0 {
		w.b.WriteString(s)
	}
	w.l += len(s)
}

func (w *twoPassWriter) writeByte(b byte) {
	if w.b.Cap() != 0 {
		w.b.WriteByte(b)
	}
	w.l += 1
}
