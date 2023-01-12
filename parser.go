package main

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"unicode/utf8"
)

type parser struct {
	b              bytes.Buffer
	newlineOffsets []int
	declarations   []declaration

	tempBuffer tempBuffer
}

var errInvalidUtf8 = errors.New("invalid utf8")

func (p *parser) init(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	p.b.Reset()
	if _, err := p.b.ReadFrom(f); err != nil {
		return err
	}
	if !utf8.Valid(p.b.Bytes()) {
		// TODO: get invalid rune position
		return errInvalidUtf8
	}
	// TODO: check against cache? -> sum := sha256.Sum256(b.Bytes())
	p.calculateNewlineOffsets()
	return nil
}

func (p *parser) calculateNewlineOffsets() {
	p.newlineOffsets = p.newlineOffsets[:0]
	offset := 0
	b := p.b.Bytes()
	for {
		i := bytes.IndexByte(b[offset:], '\n')
		if i < 0 {
			return
		}
		p.newlineOffsets = append(p.newlineOffsets, offset+i)
		offset += i + 1
	}
}

var errLineIndexOutOfBounds = errors.New("line index out of bounds")

func (p *parser) lineCount() int {
	return len(p.newlineOffsets) + 1
}

func (p *parser) lineAt(index int) []byte {
	from, to := p.lineAtRange(index)
	return p.b.Bytes()[from:to]
}

func (p *parser) lineSlice(from, toInclusive int) []byte {
	sliceFrom, _ := p.lineAtRange(from)
	_, sliceTo := p.lineAtRange(toInclusive)
	return p.b.Bytes()[sliceFrom:sliceTo]
}

func (p *parser) lineAtRange(index int) (from, to int) {
	if index < 0 || index > len(p.newlineOffsets) {
		panic(errLineIndexOutOfBounds)
	}

	b := p.b.Bytes()
	if index == len(p.newlineOffsets) {
		if len(p.newlineOffsets) == 0 {
			return 0, len(b)
		}
		from := p.newlineOffsets[len(p.newlineOffsets)-1] + 1
		return from, len(b)
	}
	from = 0
	if index != 0 {
		from = p.newlineOffsets[index-1] + 1
	}
	to = p.newlineOffsets[index] + 1
	return from, to
}

type parserError struct {
	line int // starts at 1
	msg  string
}

func (e *parserError) Error() string {
	return fmt.Sprintf("line %d: %s", e.line, e.msg)
}

const headerStartComment = "--- "

func (p *parser) buildRawDeclarations() error {
	// We try to handle comments and strings in a database general way.
	// Double and single quotes can span multiple lines. Repetition esacpes them.
	// Block comments can not be nested, but line comments can.
	// Block comments can contain line comments.
	// Whatever comes first takes precedence over the rest.

	p.declarations = p.declarations[:0]
	insideDeclarationBlock := false
	state := markerNone
	lastMarkerLineIndex := -1 // for error reporting

	for lineIndex := 0; lineIndex < p.lineCount(); lineIndex++ {
		line := p.lineAt(lineIndex)
		trimmed := bytes.TrimSpace(line)

		if insideDeclarationBlock && len(trimmed) == 0 {
			p.declarations[len(p.declarations)-1].endLineIndex = lineIndex
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
					p.declarations = append(p.declarations, declaration{
						startLineIndex: lineIndex,
						endLineIndex:   p.lineCount() - 1,
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
