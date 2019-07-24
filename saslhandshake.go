package kafka

// saslHandshakeRequestV0 implements the format for V0 and V1 SASL
// requests (they are identical)
type saslHandshakeRequestV0 struct {
	// Mechanism holds the SASL Mechanism chosen by the client.
	Mechanism string
}

func (t saslHandshakeRequestV0) size() int32 {
	return sizeofString(t.Mechanism)
}

func (t *saslHandshakeRequestV0) readFrom(rb *readBuffer) {
	t.Mechanism = rb.readString()
}

func (t saslHandshakeRequestV0) writeTo(wb *writeBuffer) {
	wb.writeString(t.Mechanism)
}

// saslHandshakeResponseV0 implements the format for V0 and V1 SASL
// responses (they are identical)
type saslHandshakeResponseV0 struct {
	// ErrorCode holds response error code
	ErrorCode int16

	// Array of mechanisms enabled in the server
	EnabledMechanisms []string
}

func (t saslHandshakeResponseV0) size() int32 {
	return sizeofInt16(t.ErrorCode) + sizeofStringArray(t.EnabledMechanisms)
}

func (t saslHandshakeResponseV0) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
	wb.writeStringArray(t.EnabledMechanisms)
}

func (t *saslHandshakeResponseV0) readFrom(rb *readBuffer) {
	t.ErrorCode = rb.readInt16()
	t.EnabledMechanisms = rb.readStringArray()
}
