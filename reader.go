package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
)

type reader struct {
	c *conn
	r *bufio.Reader

	// b and orig reference the buffer of r

	b              []byte
	originalBuffer []byte
}

const readBufferSize = 4096 * 2 * 10

func newReader(c *conn, r io.Reader) *reader {
	return &reader{c: c, r: bufio.NewReaderSize(r, readBufferSize)}
}

func (r *reader) readMessage() error {
	for {
		if _, err := r.r.Discard(len(r.originalBuffer)); err != nil {
			panic(err)
		}

		header, err := r.r.Peek(5)
		if err != nil {
			return fmt.Errorf("unable to read next message header: %w", err)
		}
		kind := header[0]
		length := binary.BigEndian.Uint32(header[1:])
		n, err := safeConvert[int64, int](int64(length) + 1)
		if err != nil {
			return err
		}

		b, err := r.r.Peek(n)
		if err != nil {
			return err
		}
		r.b = b
		r.originalBuffer = b

		switch kind {
		case 'E':
			errPq, err := r.errorResponse()
			if err != nil {
				return err
			}
			return errPq
		case 'S':
			if length == 4 {
				// PortalSuspended
				return nil
			} else {
				if err := r.parameterStatus(); err != nil {
					return err
				}
				continue
			}
		case 'N':
			n, err := r.noticeReponse()
			if err != nil {
				return err
			}
			log.Printf("%s", n)
			continue
		case 'A':
			return errors.New("NotificationResponse not implemented")
		default:
			return nil
		}
	}
}

func (r *reader) peekKind() (byte, error) {
	if len(r.b) != len(r.originalBuffer) {
		panic("called peekKind after read")
	}
	if len(r.b) < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	return r.b[0], nil
}

func (r *reader) expectKind(expected byte) error {
	if len(r.b) < 1 {
		return io.ErrUnexpectedEOF
	}
	got := r.b[0]
	r.b = r.b[1:]
	if got != expected {
		return &errorUnexpectedKind{
			expected: expected,
			got:      got,
		}
	}
	return nil
}

func (r *reader) readByte() (byte, error) {
	if len(r.b) < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	b := r.b[0]
	r.b = r.b[1:]
	return b, nil
}

func (r *reader) readInt16() (int, error) {
	if len(r.b) < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	u16 := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return int(int16(u16)), nil
}

func (r *reader) readInt32() (int, error) {
	if len(r.b) < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	u32 := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return int(int32(u32)), nil
}

var errNegativeByteReadCount = errors.New("negative byte read count")

func (r *reader) readBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, errNegativeByteReadCount
	}
	if len(r.b) < n {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.b[:n]
	r.b = r.b[n:]
	return b, nil
}

func (r *reader) readString() ([]byte, error) {
	i, err := nullByteIndex(r.b)
	if err != nil {
		return nil, err
	}
	s := r.b[:i]
	r.b = r.b[i+1:]
	return s, nil
}

func nullByteIndex(b []byte) (int, error) {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return -1, errContainsNoNullByte
	}
	return i, nil
}

func (r *reader) parseComplete() error {
	return r.expectKind('1')
}

func (r *reader) authenticationOk() error {
	if err := r.expectKind('R'); err != nil {
		return err
	}
	if _, err := r.readInt32(); err != nil {
		return err
	}
	authCode, err := r.readInt32()
	if err != nil {
		return err
	}
	if authCode != 0 {
		return fmt.Errorf("not implemented authentication method %d requested", authCode)
	}
	return nil
}

func (r *reader) errorAndNoticeResponse(out *errorAndNoticeFields) error {
	if _, err := r.readInt32(); err != nil {
		return err
	}

	for {
		typ, err := r.readByte()
		if err != nil {
			return err
		}
		if typ == 0 {
			return nil
		}
		value, err := r.readString()
		if err != nil {
			return err
		}
		out.assignField(typ, string(value))
	}
}

func (r *reader) errorResponse() (*postgresError, error) {
	if err := r.expectKind('E'); err != nil {
		return nil, err
	}
	var errPq postgresError
	if err := r.errorAndNoticeResponse(&errPq.errorAndNoticeFields); err != nil {
		return nil, err
	}
	return &errPq, nil
}

func (r *reader) noticeReponse() (*notice, error) {
	if err := r.expectKind('N'); err != nil {
		return nil, err
	}
	var n notice
	if err := r.errorAndNoticeResponse(&n.errorAndNoticeFields); err != nil {
		return nil, err
	}
	return &n, nil
}

func (r *reader) parameterStatus() error {
	if err := r.expectKind('S'); err != nil {
		return err
	}
	if _, err := r.readInt32(); err != nil {
		return err
	}
	parameter, err := r.readString()
	if err != nil {
		return err
	}
	value, err := r.readString()
	if err != nil {
		return err
	}
	r.c.parameterStatuses[string(parameter)] = string(value)
	return nil
}

func (r *reader) backendKeyData() error {
	if err := r.expectKind('K'); err != nil {
		return err
	}
	if _, err := r.readInt32(); err != nil {
		return err
	}
	processId, err := r.readInt32()
	if err != nil {
		return err
	}
	secretKey, err := r.readInt32()
	if err != nil {
		return err
	}
	r.c.processId, r.c.secretKey = processId, secretKey
	return nil
}

func (r *reader) readyForQuery() error {
	if err := r.expectKind('Z'); err != nil {
		return err
	}
	if _, err := r.readInt32(); err != nil {
		return err
	}
	txStatus, err := r.readByte()
	if err != nil {
		return err
	}
	r.c.txStatus = txStatus
	return nil
}

func (r *reader) parameterDescription() error {
	if err := r.expectKind('t'); err != nil {
		return err
	}
	if _, err := r.readInt32(); err != nil {
		return err
	}
	parameterLength, err := r.readInt16()
	if err != nil {
		return err
	}

	r.c.currentParameterOids = r.c.currentParameterOids[:0]
	for i := 0; i < parameterLength; i++ {
		oid, err := r.readInt32()
		if err != nil {
			return err
		}
		r.c.currentParameterOids = append(r.c.currentParameterOids, oid)
	}

	return nil
}

func (r *reader) rowDescription() error {
	if err := r.expectKind('T'); err != nil {
		return err
	}
	if _, err := r.readInt32(); err != nil {
		return err
	}
	fieldsLength, err := r.readInt16()
	if err != nil {
		return err
	}

	r.c.currentFields = r.c.currentFields[:0]
	r.c.currentFieldNames = r.c.currentFieldNames[:0]
	for i := 0; i < fieldsLength; i++ {
		name, err := r.readString()
		if err != nil {
			return err
		}
		maybeTableOid, err := r.readInt32()
		if err != nil {
			return err
		}
		maybeColumnAttributeNumber, err := r.readInt16()
		if err != nil {
			return err
		}
		typeOid, err := r.readInt32()
		if err != nil {
			return err
		}
		typeSize, err := r.readInt16() // pg_type.typlen
		if err != nil {
			return err
		}
		typeModifier, err := r.readInt32() // pg_attribute.atttypmod
		if err != nil {
			return err
		}
		formatCode, err := r.readInt16()
		if err != nil {
			return err
		}

		r.c.currentFieldNames = append(r.c.currentFieldNames, name...)
		// if currentFieldNames grows during append, field.name references an old buffer
		storedName := r.c.currentFieldNames[len(r.c.currentFieldNames)-len(name):]
		f := field{
			name:                       storedName,
			maybeTableOid:              maybeTableOid,
			maybeColumnAttributeNumber: maybeColumnAttributeNumber,
			typeOid:                    typeOid,
			typeSize:                   typeSize,
			typeModifier:               typeModifier,
			formatCode:                 formatCode,
		}
		r.c.currentFields = append(r.c.currentFields, f)
	}

	if cap(r.c.currentDataFields) < cap(r.c.currentFields) {
		r.c.currentDataFields = make([]dataField, len(r.c.currentFields), cap(r.c.currentFields))
	} else {
		r.c.currentDataFields = r.c.currentDataFields[:len(r.c.currentFields)]
	}
	return nil
}

type dataField struct {
	isNull bool
	value  []byte
}

func (r *reader) dataRow() error {
	if err := r.expectKind('D'); err != nil {
		return err
	}
	if _, err := r.readInt32(); err != nil {
		return err
	}
	columnsLength, err := r.readInt16()
	if err != nil {
		return err
	}
	dataFields := r.c.currentDataFields
	if columnsLength != len(dataFields) {
		return fmt.Errorf("expected %d columns, got %d", len(dataFields), columnsLength)
	}

	for i := range dataFields {
		valueLength, err := r.readInt32()
		if err != nil {
			return err
		}
		if valueLength < 0 {
			dataFields[i].isNull = true
			dataFields[i].value = nil
		} else {
			dataFields[i].isNull = false
			value, err := r.readBytes(valueLength)
			if err != nil {
				return err
			}
			dataFields[i].value = value
		}
	}

	return nil
}

type commandTagReader struct {
	b []byte
}

var errMalformedCommandTag = errors.New("malformed command tag")

func (r *commandTagReader) readSegment() (segment []byte, err error) {
	if len(r.b) == 0 {
		return nil, errMalformedCommandTag
	}
	segmentLength := bytes.IndexByte(r.b, ' ')
	if segmentLength < 0 {
		segment := r.b
		r.b = nil
		return segment, nil
	}
	segment = r.b[:segmentLength]
	r.b = r.b[segmentLength+1:]
	return segment, nil
}

func parseUint64(b []byte) (uint64, error) {
	n := uint64(0)
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid number %q", b)
		}

		lastN := n
		n *= 10
		d := uint64(ch - '0')
		n += uint64(d)
		if n < lastN {
			return 0, fmt.Errorf("number %q overflows uint64", b)
		}
	}
	return n, nil
}

type commandType int

const (
	commandUnknown commandType = iota
	commandInsert
	commandDelete
	commandUpdate
	commandSelect
	commandMove
	commandFetch
	commandCopy
	commandLength
)

var commandTypes = []string{
	commandUnknown: "UNKNOWN",
	commandInsert:  "INSERT",
	commandDelete:  "DELETE",
	commandUpdate:  "UPDATE",
	commandSelect:  "SELECT",
	commandMove:    "MOVE",
	commandFetch:   "FETCH",
	commandCopy:    "COPY",
}

var commandTypesMapping = make(map[string]commandType)

func init() {
	for command, s := range commandTypes {
		commandTypesMapping[s] = commandType(command)
	}
}

func (c commandType) String() string {
	if c < 0 || c >= commandLength {
		return commandTypes[commandUnknown]
	}
	return commandTypes[c]
}

func (r *reader) commandComplete() (command commandType, rows uint64, err error) {
	if err := r.expectKind('C'); err != nil {
		return commandUnknown, 0, err
	}
	if _, err := r.readInt32(); err != nil {
		return commandUnknown, 0, err
	}

	commandTag, err := r.readString()
	if err != nil {
		return commandUnknown, 0, err
	}
	cr := commandTagReader{commandTag}
	commandRaw, err := cr.readSegment()
	if err != nil {
		return commandUnknown, 0, err
	}
	command, ok := commandTypesMapping[string(commandRaw)]
	if !ok {
		return commandUnknown, 0, fmt.Errorf("unknown command type %q", commandRaw)
	}

	if command == commandInsert {
		// skip unused oid field
		if _, err := cr.readSegment(); err != nil {
			return commandUnknown, 0, err
		}
	}

	rowsRaw, err := cr.readSegment()
	if err != nil {
		return commandUnknown, 0, err
	}
	rows, err = parseUint64(rowsRaw)
	if err != nil {
		return commandUnknown, 0, err
	}

	return command, rows, nil
}

type errorUnexpectedKind struct {
	expected byte
	got      byte
}

func (e *errorUnexpectedKind) Error() string {
	return fmt.Sprintf(
		"unexpected message kind: expected %c, got %c",
		e.expected,
		e.got,
	)
}
