package kafka

import (
	"bufio"
)

type saslAuthenticateRequestV1 struct {
	// Data holds the SASL payload
	Data []byte
}

func (t saslAuthenticateRequestV1) size() int32 {
	return sizeofBytes(t.Data)
}

func (t *saslAuthenticateRequestV1) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	return readBytes(r, sz, &t.Data)
}

func (t saslAuthenticateRequestV1) writeTo(wb *writeBuffer) {
	wb.writeBytes(t.Data)
}

type saslAuthenticateResponseV1 struct {
	ErrorCode int16

	ErrorMessage string

	Data []byte

	SessionLifeTimeMs int64
}

func (t saslAuthenticateResponseV1) size() int32 {
	return sizeofInt16(t.ErrorCode) + sizeofString(t.ErrorMessage) + sizeofBytes(t.Data) + sizeofInt64(t.SessionLifeTimeMs)
}

func (t saslAuthenticateResponseV1) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
	wb.writeString(t.ErrorMessage)
	wb.writeBytes(t.Data)
	wb.writeInt64(t.SessionLifeTimeMs)
}

func (t *saslAuthenticateResponseV1) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt16(r, sz, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ErrorMessage); err != nil {
		return
	}
	if remain, err = readBytes(r, remain, &t.Data); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &t.SessionLifeTimeMs); err != nil {
		return
	}

	return
}
