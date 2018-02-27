package kafka

import (
	"bufio"
	"bytes"
)

type memberGroupMetadata struct {
	// MemberID assigned by the group coordinator or null if joining for the
	// first time.
	MemberID string
	Metadata groupMetadata
}

type groupMetadata struct {
	Version  int16
	Topics   []string
	UserData []byte
}

func (t groupMetadata) size() int32 {
	return sizeofInt16(t.Version) +
		sizeofStringArray(t.Topics) +
		sizeofBytes(t.UserData)
}

func (t groupMetadata) writeTo(w *bufio.Writer) {
	writeInt16(w, t.Version)
	writeStringArray(w, t.Topics)
	writeBytes(w, t.UserData)
}

func (t groupMetadata) bytes() []byte {
	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	t.writeTo(w)
	w.Flush()
	return buf.Bytes()
}

func (t *groupMetadata) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &t.Version); err != nil {
		return
	}
	if remain, err = readStringArray(r, remain, &t.Topics); err != nil {
		return
	}
	if remain, err = readBytes(r, remain, &t.UserData); err != nil {
		return
	}
	return
}
