package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// TODO: handle 55.2.7. Asynchronous Operations

type reader struct {
	r *bufio.Reader

	// reference the buffer of r
	b              []byte
	originalBuffer []byte
}

const readBufferSize = 4096 * 2 * 10

func newReader(r io.Reader) *reader {
	return &reader{r: bufio.NewReaderSize(r, readBufferSize)}
}

func (r *reader) readMessage() error {
	if _, err := r.r.Discard(len(r.originalBuffer)); err != nil {
		panic(err)
	}

	header, err := r.r.Peek(5)
	if err != nil {
		return fmt.Errorf("unable to read next message header: %w", err)
	}
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

	errPq := r.newError()
	if errPq == nil {
		r.resetToOriginal()
		return nil
	}
	return errPq
}

func (r *reader) newError() error {
	if err := r.expectKind('E'); err != nil {
		if unexpectedKind := (*errorUnexpectedKind)(nil); errors.As(err, &unexpectedKind) {
			return nil
		}
		return r.newErrorParseFailed(err)
	}

	if _, err := r.readInt32(); err != nil {
		return r.newErrorParseFailed(err)
	}

	var errPq errorPostgres
	for {
		typ, err := r.readByte()
		if err != nil {
			return r.newErrorParseFailed(err)
		}
		if typ == 0 {
			return &errPq
		}
		value, err := r.readString()
		if err != nil {
			return r.newErrorParseFailed(err)
		}
		errPq.assignField(typ, string(value))
	}
}

func (r *reader) newErrorParseFailed(err error) error {
	return fmt.Errorf("%w (%q)", err, r.originalBuffer)
}

func (r *reader) resetToOriginal() {
	r.b = r.originalBuffer
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
		return fmt.Errorf("authentication method %d failed: not implemented", authCode)
	}
	return nil
}

func (r *reader) parameterStatus() (parameter []byte, value []byte, err error) {
	if err := r.expectKind('S'); err != nil {
		return nil, nil, err
	}
	if _, err := r.readInt32(); err != nil {
		return nil, nil, err
	}
	parameter, err = r.readString()
	if err != nil {
		return nil, nil, err
	}
	value, err = r.readString()
	if err != nil {
		return nil, nil, err
	}
	return parameter, value, nil
}

func (r *reader) backendKeyData() (processId, secretKey int, err error) {
	if err := r.expectKind('K'); err != nil {
		return 0, 0, err
	}
	if _, err := r.readInt32(); err != nil {
		return 0, 0, err
	}
	processId, err = r.readInt32()
	if err != nil {
		return 0, 0, err
	}
	secretKey, err = r.readInt32()
	if err != nil {
		return 0, 0, err
	}
	return processId, secretKey, nil
}

func (r *reader) readyForQuery() (txStatus byte, err error) {
	if err := r.expectKind('Z'); err != nil {
		return 0, err
	}
	if _, err := r.readInt32(); err != nil {
		return 0, err
	}
	txStatus, err = r.readByte()
	if err != nil {
		return 0, err
	}
	return txStatus, nil
}

func (r *reader) parameterDescription() ([]int, error) {
	if err := r.expectKind('t'); err != nil {
		return nil, err
	}
	if _, err := r.readInt32(); err != nil {
		return nil, err
	}
	parameterLength, err := r.readInt16()
	if err != nil {
		return nil, err
	}
	oids := make([]int, parameterLength)
	for i := 0; i < parameterLength; i++ {
		oid, err := r.readInt32()
		if err != nil {
			return nil, err
		}
		oids[i] = oid
	}
	return oids, nil
}

type field struct {
	name string

	maybeTableOid              int
	maybeColumnAttributeNumber int

	typeOid      int
	typeSize     int
	typeModifier int
}

func (r *reader) rowDescription() ([]field, error) {
	if err := r.expectKind('T'); err != nil {
		return nil, err
	}
	if _, err := r.readInt32(); err != nil {
		return nil, err
	}
	fieldsLength, err := r.readInt16()
	if err != nil {
		return nil, err
	}

	fields := make([]field, fieldsLength)
	for i := 0; i < fieldsLength; i++ {
		name, err := r.readString()
		if err != nil {
			return nil, err
		}
		maybeTableOid, err := r.readInt32()
		if err != nil {
			return nil, err
		}
		maybeColumnAttributeNumber, err := r.readInt16()
		if err != nil {
			return nil, err
		}
		typeOid, err := r.readInt32()
		if err != nil {
			return nil, err
		}
		typeSize, err := r.readInt16() // pg_type.typlen
		if err != nil {
			return nil, err
		}
		typeModifier, err := r.readInt32() // pg_attribute.atttypmod
		if err != nil {
			return nil, err
		}
		// format code
		if _, err := r.readInt16(); err != nil {
			return nil, err
		}

		fields[i] = field{
			name:                       string(name),
			maybeTableOid:              maybeTableOid,
			maybeColumnAttributeNumber: maybeColumnAttributeNumber,
			typeOid:                    typeOid,
			typeSize:                   typeSize,
			typeModifier:               typeModifier,
		}
	}

	return fields, nil
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
