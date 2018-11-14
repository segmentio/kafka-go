package kafka

import (
	"bufio"
)

type apiVersionsRequestV1 struct {
}

func (t apiVersionsRequestV1) size() int32 {
	return 0
}

func (t apiVersionsRequestV1) writeTo(w *bufio.Writer) {}

type apiVersionsResponseV1Range struct {
	APIKey int16

	MinVersion int16
	MaxVersion int16
}

func (t *apiVersionsResponseV1Range) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt16(r, sz, &t.APIKey); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.MinVersion); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.MaxVersion); err != nil {
		return
	}
	return
}

func (t apiVersionsResponseV1Range) writeTo(w *bufio.Writer) {
	writeInt16(w, t.APIKey)
	writeInt16(w, t.MinVersion)
	writeInt16(w, t.MaxVersion)
}

func (t *apiVersionsResponseV1Range) size() int32 { return 6 }

type apiVersionsResponseV1 struct {
	// ErrorCode holds response error code
	ErrorCode int16

	// Responses holds api versions per api key
	APIVersions []apiVersionsResponseV1Range

	// ThrottleTimeMS holds the duration in milliseconds for which the request
	// was throttled due to quota violation (Zero if the request did not violate
	// any quota)
	ThrottleTimeMS int32
}

func (t apiVersionsResponseV1) size() int32 {
	return sizeofInt16(t.ErrorCode) +
		sizeofArray(len(t.APIVersions), func(i int) int32 { return t.APIVersions[i].size() }) +
		sizeofInt32(t.ThrottleTimeMS)
}

func (t apiVersionsResponseV1) writeTo(w *bufio.Writer) {
	writeInt16(w, t.ErrorCode)
	writeArray(w, len(t.APIVersions), func(i int) { t.APIVersions[i].writeTo(w) })
	writeInt32(w, t.ThrottleTimeMS)
}

func (t *apiVersionsResponseV1) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt16(r, sz, &t.ErrorCode); err != nil {
		return
	}
	fn := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		item := apiVersionsResponseV1Range{}
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.APIVersions = append(t.APIVersions, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &t.ThrottleTimeMS); err != nil {
		return
	}
	return
}
