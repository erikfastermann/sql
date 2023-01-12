package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
)

type builder struct {
	b            []byte
	lengthOffset int
	firstError   error
}

func (b *builder) reset() {
	b.firstError = nil
	b.b = b.b[:0]
	b.lengthOffset = 0
}

func (b *builder) newMessage(kind byte) {
	if b.firstError != nil {
		panic(b.firstError)
	}
	b.b = append(b.b, kind)
	b.lengthOffset = len(b.b)
	b.b = append(b.b, 0, 0, 0, 0) // message length, set later
}

func (b *builder) newMessageLengthOnly() {
	if b.firstError != nil {
		panic(b.firstError)
	}
	b.lengthOffset = len(b.b)
	b.b = append(b.b, 0, 0, 0, 0) // message length, set later
}

func (b *builder) appendByte(byt byte) {
	if b.firstError != nil {
		return
	}
	b.b = append(b.b, byt)
}

func (b *builder) appendInt16(i int) {
	if b.firstError != nil {
		return
	}
	i16, err := safeConvert[int, int16](i)
	if err != nil {
		b.firstError = err
		return
	}
	b.b = binary.BigEndian.AppendUint16(b.b, uint16(i16))
}

func (b *builder) appendInt32(i int) {
	if b.firstError != nil {
		return
	}
	i32, err := safeConvert[int, int32](i)
	if err != nil {
		b.firstError = err
		return
	}
	b.b = binary.BigEndian.AppendUint32(b.b, uint32(i32))
}

func (b *builder) appendString(s string) {
	if !b.checkContainsNoNull(s) {
		return
	}
	b.b = append(b.b, s...)
	b.b = append(b.b, 0)
}

func (b *builder) appendBytes(p []byte) {
	if !b.checkBytesContainsNoNull(p) {
		return
	}
	b.b = append(b.b, p...)
	b.b = append(b.b, 0)
}

func (b *builder) appendRawString(s string) {
	if b.firstError != nil {
		return
	}
	b.b = append(b.b, s...)
}

func (b *builder) finalizeMessage() error {
	if b.firstError != nil {
		return b.firstError
	}
	l, err := safeConvert[int, uint32](len(b.b) - b.lengthOffset)
	if err != nil {
		b.firstError = err
		return err
	}
	binary.BigEndian.PutUint32(b.b[b.lengthOffset:], l)
	return nil
}

var (
	errContainsNoNullByte = errors.New("string contains no null byte")
	errContainsNullByte   = errors.New("string contains null byte")
)

func (w *builder) checkContainsNoNull(s string) bool {
	if w.firstError != nil {
		return false
	}
	if strings.IndexByte(s, 0) >= 0 {
		w.firstError = errContainsNullByte
		return false
	}
	return true
}

func (w *builder) checkBytesContainsNoNull(b []byte) bool {
	if w.firstError != nil {
		return false
	}
	if bytes.IndexByte(b, 0) >= 0 {
		w.firstError = errContainsNullByte
		return false
	}
	return true
}

func (b *builder) startup() error {
	b.newMessageLengthOnly()

	const protocolVersion = 196608
	b.appendInt32(protocolVersion)

	b.appendString("user")
	b.appendString(postgresUser)

	b.appendString("database")
	b.appendString(postgresDb)

	b.appendByte(0)

	return b.finalizeMessage()
}

func (b *builder) saslInitialResponseScramSha256(initialResponse string) error {
	b.newMessage('p')
	b.appendString(string(saslAuthMechanismScramSha256))
	if len(initialResponse) == 0 {
		b.appendInt32(-1)
	} else {
		b.appendInt32(len(initialResponse))
	}
	b.appendRawString(initialResponse)
	return b.finalizeMessage()
}

func (b *builder) saslResponse(data string) error {
	b.newMessage('p')
	b.appendRawString(data)
	return b.finalizeMessage()
}

func (b *builder) parse(preparedStatement, query string) error {
	b.newMessage('P')
	b.appendString(preparedStatement)
	b.appendString(query)
	b.appendInt16(0) // specified parameter types
	return b.finalizeMessage()
}

func (b *builder) describeStatement(preparedStatement string) error {
	b.newMessage('D')
	b.b = append(b.b, 'S')
	b.appendString(preparedStatement)
	return b.finalizeMessage()
}

func (b *builder) sync() {
	b.newMessage('S')
	if err := b.finalizeMessage(); err != nil {
		panic(err)
	}
}

func (b *builder) terminate() {
	b.newMessage('X')
	if err := b.finalizeMessage(); err != nil {
		panic(err)
	}
}

func (b *builder) query(query string) error {
	b.newMessage('Q')
	b.appendString(query)
	return b.finalizeMessage()
}
