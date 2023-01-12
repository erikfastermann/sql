package main

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"unicode/utf8"
)

// TODO: rename to parser

type handler struct {
	b              bytes.Buffer
	newlineOffsets []int
	declarations   []declaration
}

var errInvalidUtf8 = errors.New("invalid utf8")

func (h *handler) init(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h.b.Reset()
	if _, err := h.b.ReadFrom(f); err != nil {
		return err
	}
	if !utf8.Valid(h.b.Bytes()) {
		// TODO: get invalid rune position
		return errInvalidUtf8
	}
	// TODO: check against cache? -> sum := sha256.Sum256(b.Bytes())
	h.calculateNewlineOffsets()
	return nil
}

func (h *handler) calculateNewlineOffsets() {
	h.newlineOffsets = h.newlineOffsets[:0]
	offset := 0
	b := h.b.Bytes()
	for {
		i := bytes.IndexByte(b[offset:], '\n')
		if i < 0 {
			return
		}
		h.newlineOffsets = append(h.newlineOffsets, offset+i)
		offset += i + 1
	}
}

var errLineIndexOutOfBounds = errors.New("line index out of bounds")

func (h *handler) lineCount() int {
	return len(h.newlineOffsets) + 1
}

func (h *handler) lineAt(index int) []byte {
	if index < 0 || index > len(h.newlineOffsets) {
		panic(errLineIndexOutOfBounds)
	}
	b := h.b.Bytes()
	if index == len(h.newlineOffsets) {
		if len(h.newlineOffsets) == 0 {
			return b
		}
		start := h.newlineOffsets[len(h.newlineOffsets)-1] + 1
		return b[start:]
	}
	start := 0
	if index != 0 {
		start = h.newlineOffsets[index-1] + 1
	}
	end := h.newlineOffsets[index] + 1
	return b[start:end]
}

type parserError struct {
	line int // starts at 1
	msg  string
}

func (e *parserError) Error() string {
	return fmt.Sprintf("line %d: %s", e.line, e.msg)
}

const headerStartComment = "--- "

func (h *handler) buildDeclarations() error {
	// We try to handle comments and strings in a database general way.
	// Double and single quotes can span multiple lines. Repetition esacpes them.
	// Block comments can not be nested, but line comments can.
	// Block comments can contain line comments.
	// Whatever comes first takes precedence over the rest.

	h.declarations = h.declarations[:0]
	insideDeclarationBlock := false
	state := markerNone
	lastMarkerLineIndex := -1 // for error reporting

	for lineIndex := 0; lineIndex < h.lineCount(); lineIndex++ {
		line := h.lineAt(lineIndex)
		trimmed := bytes.TrimSpace(line)

		if insideDeclarationBlock && len(trimmed) == 0 {
			h.declarations[len(h.declarations)-1].endLineIndex = lineIndex
			insideDeclarationBlock = false
			continue
		}

		remainder := line
		for {
			if len(remainder) == 0 {
				break
			}

			switch state {
			case markerNone:
				m, pos := getFirstMarker(remainder)
				if m == markerNone {
					remainder = nil
				} else {
					state, remainder = m, remainder[pos+1:]
					lastMarkerLineIndex = lineIndex
				}
			case markerSingleQuote:
				newRemainder, stateChange := nextQuoted(remainder, '\'')
				remainder = newRemainder
				if stateChange {
					state = markerNone
				}
			case markerDoubleQuote:
				newRemainder, stateChange := nextQuoted(remainder, '"')
				remainder = newRemainder
				if stateChange {
					state = markerNone
				}
			case markerLineComment:
				isDeclaration := len(trimmed) >= len(headerStartComment) &&
					string(trimmed[:len(headerStartComment)]) == headerStartComment
				if isDeclaration {
					if insideDeclarationBlock {
						return &parserError{
							line: lineIndex + 1,
							msg:  "declaration blocks must be separated by a blank line",
						}
					}
					h.declarations = append(h.declarations, declaration{
						startLineIndex: lineIndex,
						endLineIndex:   h.lineCount() - 1,
						header:         bytes.TrimSpace(trimmed[len(headerStartComment):]),
					})
					insideDeclarationBlock = true
				}
				state = markerNone
				remainder = nil
			case markerBlockCommentStart:
				blockCommentEndIndex := bytes.Index(remainder, []byte("*\\"))
				if blockCommentEndIndex < 0 {
					remainder = nil
				} else {
					state = markerNone
					remainder = remainder[blockCommentEndIndex+2:]
				}
			default:
				panic("unreachable")
			}
		}
	}

	switch state {
	case markerNone:
		return nil
	case markerSingleQuote:
		return &parserError{
			line: lastMarkerLineIndex + 1,
			msg:  "unterminated single quote `'`",
		}
	case markerDoubleQuote:
		return &parserError{
			line: lastMarkerLineIndex + 1,
			msg:  "unterminated double quote `\"`",
		}
	case markerLineComment:
		panic("unreachable")
	case markerBlockCommentStart:
		return &parserError{
			line: lastMarkerLineIndex + 1,
			msg:  "unterminated block comment `/*`",
		}
	default:
		panic("unreachable")
	}
}

type marker int

const (
	markerNone marker = iota
	markerSingleQuote
	markerDoubleQuote
	markerLineComment
	markerBlockCommentStart
)

func getFirstMarker(remainder []byte) (marker, int) {
	markers := [...]struct {
		m     marker
		index int
	}{
		{markerSingleQuote, bytes.IndexByte(remainder, '\'')},
		{markerDoubleQuote, bytes.IndexByte(remainder, '"')},
		{markerLineComment, bytes.Index(remainder, []byte("--"))},
		{markerBlockCommentStart, bytes.Index(remainder, []byte("\\*"))},
	}

	selected := markerNone
	firstIndex := math.MaxInt
	for _, m := range markers {
		if m.index >= 0 && m.index < firstIndex {
			selected = m.m
			firstIndex = m.index
		}
	}

	return selected, firstIndex
}

func nextQuoted(remainder []byte, quote byte) (newRemainder []byte, stateChange bool) {
	index := bytes.IndexByte(remainder, quote)
	if index < 0 {
		return nil, false
	}
	isLast := index == len(remainder)-1
	if isLast {
		return nil, true
	}
	escaped := remainder[index+1] == quote
	if escaped {
		return remainder[index+2:], false
	} else {
		return remainder[index+1:], true
	}
}

// TODO:
//   - dependency tracking
//   - check valid go identifier

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
	endLineIndex int
	header       []byte

	// the following fields are set by calling parse

	resultKind              resultKind
	resultCount             resultCount
	resultStructHasFuncName bool

	funcName      []byte // might be empty if resultKind == resultStruct
	structName    []byte // only set if resultKind == resultStruct
	columnOptions []columnOption
}

const (
	regexpIdentifier          = `(\pL+[\pL\pN]*)`
	regexpIdentifierWithEdges = `(([#!]?)` + regexpIdentifier + `([\?\+]?))`
	regexpTwoNames            = "(" + regexpIdentifier + " -> " + regexpIdentifierWithEdges + ")"
	regexpHeader              = "^" + regexpTwoNames + "|" + regexpIdentifierWithEdges
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
	errHeaderExtraContent = errors.New("unexpected extra content in header") // TODO: include string
)

func (d *declaration) parseHeader() error {
	// TODO: real parser

	match := headerMatcher.FindSubmatch(d.header)
	if len(match) == 0 {
		return errInvalidHeader
	}
	if len(match) != 11 {
		panic("internal error")
	}

	const (
		reTwoNames             = 1
		reTwoNamesFuncName     = 2
		reTwoNamesEdgesStart   = 3
		reSingleNameEdgesStart = 7
	)
	const (
		rePrefixOffset = 1
		reNameOffset   = 2
		reSuffixOffset = 3
	)

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

	// TODO: check no remaining content in header
	// TODO: column nullability spec

	return nil
}

type columnOption struct {
	index     int // starts at 1, use names if < 0
	tableName string
	name      string // column or field name
}
