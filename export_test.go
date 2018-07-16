package kafka

func (msg Message) Encode() (Message, error) {
	return msg.encode()
}

func (msg Message) Decode() (Message, error) {
	return msg.decode()
}
