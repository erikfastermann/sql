package postgres

import (
	"github.com/erikfastermann/sql/util"
)

type AdditionalErrorAndNoticeField struct {
	Identifier byte
	Value      string
}

// See https://www.postgresql.org/docs/current/protocol-error-fields.html
type ErrorAndNoticeFields struct {
	SeverityLocalized string
	Severity          string
	SqlstateCode      string
	Message           string
	MessageDetailed   string
	Hint              string
	Position          int // 0 == not set
	PositionInternal  int // 0 == not set
	QueryInternal     string
	Where             string
	SchemaName        string
	TableName         string
	ColumnName        string
	TypeName          string
	ConstraintName    string
	File              string
	Line              string
	Routine           string

	Additional []AdditionalErrorAndNoticeField
}

func (e *ErrorAndNoticeFields) assignField(typ byte, value []byte) {
	var positionRef *int
	switch typ {
	case 'P':
		positionRef = &e.Position
	case 'p':
		positionRef = &e.PositionInternal
	}
	if positionRef != nil {
		position64, err := util.ParseInt64(value)
		if err != nil {
			e.Additional = append(e.Additional, AdditionalErrorAndNoticeField{
				Identifier: typ,
				Value:      string(value),
			})
			return
		}
		position, err := util.SafeConvert[int64, int](position64)
		if err != nil {
			e.Additional = append(e.Additional, AdditionalErrorAndNoticeField{
				Identifier: typ,
				Value:      string(value),
			})
			return
		}
		e.Position = position
	}

	copied := string(value)
	switch typ {
	case 'S':
		e.SeverityLocalized = copied
	case 'V':
		e.Severity = copied
	case 'C':
		e.SqlstateCode = copied
	case 'M':
		e.Message = copied
	case 'D':
		e.MessageDetailed = copied
	case 'H':
		e.Hint = copied
	case 'q':
		e.QueryInternal = copied
	case 'W':
		e.Where = copied
	case 's':
		e.SchemaName = copied
	case 't':
		e.TableName = copied
	case 'c':
		e.ColumnName = copied
	case 'd':
		e.TypeName = copied
	case 'n':
		e.ConstraintName = copied
	case 'F':
		e.File = copied
	case 'L':
		e.Line = copied
	case 'R':
		e.Routine = copied
	default:
		e.Additional = append(e.Additional, AdditionalErrorAndNoticeField{
			Identifier: typ,
			Value:      copied,
		})
	}
}

func (e *ErrorAndNoticeFields) String() string {
	return e.Message
}

type Error struct {
	ErrorAndNoticeFields
}

func (e *Error) Error() string {
	return e.String()
}

type notice struct {
	ErrorAndNoticeFields
}
