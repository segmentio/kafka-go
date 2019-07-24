package kafka

type saslAuthenticateRequestV0 struct {
	// Data holds the SASL payload
	Data []byte
}

func (t saslAuthenticateRequestV0) size() int32 {
	return sizeofBytes(t.Data)
}

func (t *saslAuthenticateRequestV0) readFrom(rb *readBuffer) {
	t.Data = rb.readBytes()
}

func (t saslAuthenticateRequestV0) writeTo(wb *writeBuffer) {
	wb.writeBytes(t.Data)
}

type saslAuthenticateResponseV0 struct {
	// ErrorCode holds response error code
	ErrorCode int16

	ErrorMessage string

	Data []byte
}

func (t saslAuthenticateResponseV0) size() int32 {
	return sizeofInt16(t.ErrorCode) + sizeofString(t.ErrorMessage) + sizeofBytes(t.Data)
}

func (t saslAuthenticateResponseV0) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
	wb.writeString(t.ErrorMessage)
	wb.writeBytes(t.Data)
}

func (t *saslAuthenticateResponseV0) readFrom(rb *readBuffer) {
	t.ErrorCode = rb.readInt16()
	t.ErrorMessage = rb.readString()
	t.Data = rb.readBytes()
}
