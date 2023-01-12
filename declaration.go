package main

import (
	"bytes"
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/erikfastermann/sql/util"
)

type resultKind int

const (
	resultKindInvalid resultKind = iota

	resultNone // Statement returns exactly zero rows.
	// A struct is generated or referenced as the
	// return value by the generated function.
	resultStruct
	// Columns are mapped to multiple return values.
	// With resultMany, only queries that return a single column are supported.
	resultDirect

	resultKindLength
)

var resultKinds = [resultKindLength]string{
	resultKindInvalid: "invalid",
	resultNone:        "none (`!`)",
	resultStruct:      "struct",
	resultDirect:      "direct (`#`)",
}

func (r resultKind) String() string {
	if r < 0 || r >= resultKindLength {
		return resultKinds[resultKindInvalid]
	}
	return resultKinds[r]
}

type resultCount int

const (
	resultCountInvalid resultCount = iota

	resultOption // Statement returns exactly zero or one row.
	resultOne    // Statement returns exactly one row.
	resultMany   // Statement returns 0..n rows.

	resultCountLength
)

var resultCounts = [resultCountLength]string{
	resultCountInvalid: "invalid",
	resultOption:       "option (`?`)",
	resultOne:          "one",
	resultMany:         "many (`+`)",
}

func (r resultCount) String() string {
	if r < 0 || r >= resultCountLength {
		return resultCounts[resultCountInvalid]
	}
	return resultCounts[r]
}

type declaration struct {
	startLineIndex int
	// blank line or last line with data
	endLineIndex int // inclusive
	header       []byte

	// the following fields are set by calling parse

	resultKind              resultKind
	resultCount             resultCount
	resultStructHasFuncName bool

	funcName      []byte // might be empty if resultKind == resultStruct
	structName    []byte // only set if resultKind == resultStruct
	columnOptions []columnOption

	body []byte
}

func (d *declaration) String() string {
	var b strings.Builder

	if len(d.funcName) != 0 {
		b.Write(d.funcName)
		if len(d.structName) != 0 {
			b.WriteString(" -> ")
		}
	}
	if len(d.structName) != 0 {
		b.Write(d.structName)
	}
	b.WriteByte(' ')

	b.WriteByte('(')
	b.WriteString(d.resultKind.String())
	b.WriteString(" (")
	b.WriteString(d.resultCount.String())
	b.WriteString("))")

	if len(d.columnOptions) > 0 {
		b.WriteString(" [")
	}
	for i, opt := range d.columnOptions {
		if opt.index > 0 {
			b.WriteString(strconv.Itoa(opt.index))
		} else {
			if len(opt.tableName) > 0 {
				b.Write(opt.tableName)
				b.WriteByte('.')
			}
			b.Write(opt.name)
		}
		b.WriteString(": (nullable? ")
		b.WriteString(strconv.FormatBool(opt.nullable))
		b.WriteByte(')')
		if i != len(d.columnOptions)-1 {
			b.WriteString(", ")
		}
	}
	if len(d.columnOptions) > 0 {
		b.WriteString("]")
	}

	b.WriteByte('\n')
	b.Write(d.body)

	return b.String()
}

var errEmptyBody = errors.New("body of declared block is empty")

func (d *declaration) parse(h *parser) error {
	if err := d.parseHeader(&h.tempBuffer); err != nil {
		return err
	}
	if d.startLineIndex+1 >= d.endLineIndex {
		return errEmptyBody
	}
	d.body = bytes.TrimSpace(h.lineSlice(d.startLineIndex+1, d.endLineIndex))
	if bytes.HasSuffix(d.body, []byte(";")) {
		// TODO: does not work with trailing comments
		d.body = d.body[:len(d.body)-1]
	}
	return nil
}

const (
	regexpIdentifier          = `(\pL+[\pL\pN]*)`
	regexpIdentifierWithEdges = `(([#!]?)` + regexpIdentifier + `([\?\+]?))`
	regexpTwoNames            = "(" + regexpIdentifier + " -> " + regexpIdentifierWithEdges + ")"
	regexpHeader              = "^(" + regexpTwoNames + "|" + regexpIdentifierWithEdges + `)( \{(.*?)\})?$`
)

var headerMatcher = regexp.MustCompile(regexpHeader)

var (
	errInvalidHeader        = errors.New("declaration header is invalid") // TODO: nicer error
	errResultNoneWithOption = errors.New(
		"specified result kind as none with `!`, but used `?` (optional)",
	)
	errResultNoneWithMany = errors.New(
		"specified result kind as none with `!`, but used `+` (many)",
	)
	errResultNoneWithTwoNames = errors.New(
		"specified result kind as none with `!`, but used `->` (two names)",
	)
	errResultDirectWithTwoNames = errors.New(
		"specified result kind as direct with `#`, but used `->` (two names)",
	)
)

type tempBuffer struct {
	split [][]byte
}

func (d *declaration) parseHeader(t *tempBuffer) error {
	// TODO: real parser

	const (
		reTwoNames             = 2
		reTwoNamesFuncName     = 3
		reTwoNamesEdgesStart   = 4
		reSingleNameEdgesStart = 8
		reColumnOptions        = 13
		reMatchLength          = 14
	)
	const (
		rePrefixOffset = 1
		reNameOffset   = 2
		reSuffixOffset = 3
	)

	match := headerMatcher.FindSubmatch(d.header)
	if len(match) == 0 {
		return errInvalidHeader
	}
	if len(match) != reMatchLength {
		panic("internal error")
	}

	edgesStart := reSingleNameEdgesStart
	hasTwoNames := match[reTwoNames] != nil
	if hasTwoNames {
		edgesStart = reTwoNamesEdgesStart
	}

	prefix := match[edgesStart+rePrefixOffset]
	name := match[edgesStart+reNameOffset]
	suffix := match[edgesStart+reSuffixOffset]

	switch string(prefix) {
	case "!":
		if hasTwoNames {
			return errResultNoneWithTwoNames
		}
		d.resultKind = resultNone
		d.funcName = name
	case "":
		d.resultKind = resultStruct
		if hasTwoNames {
			d.resultStructHasFuncName = true
			d.funcName = match[reTwoNamesFuncName]
			d.structName = name
		} else {
			d.resultStructHasFuncName = false
			d.structName = name
		}
	case "#":
		if hasTwoNames {
			return errResultDirectWithTwoNames
		}
		d.resultKind = resultDirect
		d.funcName = name
	default:
		panic("unreachable")
	}

	switch string(suffix) {
	case "?":
		if d.resultKind == resultNone {
			return errResultNoneWithOption
		}
		d.resultCount = resultOption
	case "":
		if d.resultKind != resultNone {
			d.resultCount = resultOne
		}
	case "+":
		if d.resultKind == resultNone {
			return errResultNoneWithMany
		}
		d.resultCount = resultMany
	default:
		panic("unreachable")
	}

	columnOptionsRaw := match[reColumnOptions]
	if columnOptionsRaw != nil {
		if err := d.parseColumnOptions(columnOptionsRaw, t); err != nil {
			return err
		}
	}

	return nil
}

var (
	errColumnIndexTooLarge = errors.New("column index is too large")
	errColumnIndexTooSmall = errors.New("column index is too small (less than 1)")
)

func (d *declaration) parseColumnOptions(columnOptionsRaw []byte, t *tempBuffer) error {
	// TODO: better error messages
	// TODO: better parsing than string splitting

	t.split = t.split[:0]
	t.split = splitByteAppend(columnOptionsRaw, ',', t.split)
	for _, columnOptionRaw := range t.split {
		lastLength := len(t.split)
		t.split = splitByteAppend(columnOptionRaw, ':', t.split)
		columnOptionPairRaw := t.split[lastLength:]
		if len(columnOptionPairRaw) != 2 {
			return errInvalidHeader
		}
		specRaw, nullableRaw := columnOptionPairRaw[0], bytes.TrimSpace(columnOptionPairRaw[1])
		t.split = t.split[:lastLength]

		var nullable bool
		switch string(nullableRaw) {
		case "notnull":
			nullable = false
		case "null":
			nullable = true
		default:
			return errInvalidHeader
		}

		t.split = splitByteAppend(specRaw, ':', t.split)
		specMaybePairRaw := t.split[lastLength:]
		switch len(specMaybePairRaw) {
		case 1:
			indexOrNameRaw := bytes.TrimSpace(specMaybePairRaw[0])
			index64, err := util.ParseInt64(indexOrNameRaw)
			if err != nil {
				if errors.Is(err, util.ErrOverflow) {
					return errColumnIndexTooLarge
				}
				d.columnOptions = append(d.columnOptions, columnOption{
					index:    0,
					name:     indexOrNameRaw,
					nullable: nullable,
				})
			} else {
				index, err := util.SafeConvert[int64, int](index64)
				if err != nil {
					return errColumnIndexTooLarge
				}
				if index < 1 {
					return errColumnIndexTooSmall
				}
				d.columnOptions = append(d.columnOptions, columnOption{
					index:    index,
					nullable: nullable,
				})
			}
		case 2:
			tableRaw := bytes.TrimSpace(specMaybePairRaw[0])
			columnRaw := bytes.TrimSpace(specMaybePairRaw[1])
			d.columnOptions = append(d.columnOptions, columnOption{
				index:     0,
				tableName: tableRaw,
				name:      columnRaw,
				nullable:  nullable,
			})
		default:
			return errInvalidHeader
		}
		t.split = t.split[:lastLength]
	}

	return nil
}

func splitByteAppend(s []byte, sep byte, out [][]byte) [][]byte {
	remainder := s
	for {
		i := bytes.IndexByte(remainder, sep)
		if i < 0 {
			out = append(out, remainder)
			return out
		}
		out = append(out, remainder[:i])
		remainder = remainder[i+1:]
	}
}

type columnOption struct {
	index     int // starts at 1, use names if == 0
	tableName []byte
	name      []byte // column or field name
	nullable  bool
}
