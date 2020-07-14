package kafka

import (
	"bufio"
)

type listGroupsRequestV0 struct {
}

func (t listGroupsRequestV0) size() int32 {
	return 0
}

func (t listGroupsRequestV0) writeTo(wb *writeBuffer) {
}

type listGroupsResponseGroupV0 struct {
	// GroupID holds the unique group identifier
	GroupID      string
	ProtocolType string
}

func (t listGroupsResponseGroupV0) size() int32 {
	return sizeofString(t.GroupID) + sizeofString(t.ProtocolType)
}

func (t listGroupsResponseGroupV0) writeTo(wb *writeBuffer) {
	wb.writeString(t.GroupID)
	wb.writeString(t.ProtocolType)
}

func (t *listGroupsResponseGroupV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.GroupID); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.ProtocolType); err != nil {
		return
	}
	return
}

type listGroupsResponseV0 struct {
	// ErrorCode holds response error code
	ErrorCode int16
	Groups    []listGroupsResponseGroupV0
}

func (t listGroupsResponseV0) size() int32 {
	return sizeofInt16(t.ErrorCode) +
		sizeofArray(len(t.Groups), func(i int) int32 { return t.Groups[i].size() })
}

func (t listGroupsResponseV0) writeTo(wb *writeBuffer) {
	wb.writeInt16(t.ErrorCode)
	wb.writeArray(len(t.Groups), func(i int) { t.Groups[i].writeTo(wb) })
}

func (t *listGroupsResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &t.ErrorCode); err != nil {
		return
	}

	fn := func(withReader *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		var item listGroupsResponseGroupV0
		if fnRemain, fnErr = (&item).readFrom(withReader, withSize); err != nil {
			return
		}
		t.Groups = append(t.Groups, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}
