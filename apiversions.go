package kafka

import (
	"bufio"
)

type apiVersionsRequestV1 struct {
}

func (a apiVersionsRequestV1) size() int32 {
	return 0
}

func (a apiVersionsRequestV1) writeTo(w *bufio.Writer) {
}

type apiVersionsResponseV1 struct {
	ErrorCode      int16
	ApiVersions    []apiVersionsV1
	ThrottleTimeMs int32
}

func (a apiVersionsResponseV1) size() int32 {
	return sizeofInt16(a.ErrorCode) +
		sizeofArray(len(a.ApiVersions), func(i int) int32 { return a.ApiVersions[i].size() }) +
		sizeofInt32(a.ThrottleTimeMs)
}

func (a apiVersionsResponseV1) writeTo(w *bufio.Writer) {
	writeInt16(w, a.ErrorCode)
	writeArray(w, len(a.ApiVersions), func(i int) { a.ApiVersions[i].writeTo(w) })
	writeInt32(w, a.ThrottleTimeMs)
}

func (a *apiVersionsResponseV1) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &a.ErrorCode); err != nil {
		return
	}

	fn := func(withReader *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		var ver apiVersionsV1
		if fnRemain, fnErr = (&ver).readFrom(withReader, withSize); err != nil {
			return
		}
		a.ApiVersions = append(a.ApiVersions, ver)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	if remain, err = readInt32(r, size, &a.ThrottleTimeMs); err != nil {
		return
	}

	return
}

type apiVersionsV1 struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (a apiVersionsV1) size() int32 {
	return sizeofInt16(a.ApiKey) +
		sizeofInt16(a.MinVersion) +
		sizeofInt16(a.MaxVersion)
}

func (a apiVersionsV1) writeTo(w *bufio.Writer) {
	writeInt16(w, a.ApiKey)
	writeInt16(w, a.MinVersion)
	writeInt16(w, a.MaxVersion)
}

func (a *apiVersionsV1) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &a.ApiKey); err != nil {
		return
	}
	if remain, err = readInt16(r, size, &a.MinVersion); err != nil {
		return
	}
	if remain, err = readInt16(r, size, &a.MaxVersion); err != nil {
		return
	}

	return
}

type apiVersionsRequestV0 struct {
}

func (a apiVersionsRequestV0) size() int32 {
	return 0
}

func (a apiVersionsRequestV0) writeTo(w *bufio.Writer) {
}

type apiVersionsResponseV0 struct {
	ErrorCode   int16
	ApiVersions []apiVersionsV0
}

func (a apiVersionsResponseV0) size() int32 {
	return sizeofInt16(a.ErrorCode) +
		sizeofArray(len(a.ApiVersions), func(i int) int32 { return a.ApiVersions[i].size() })
}

func (a apiVersionsResponseV0) writeTo(w *bufio.Writer) {
	writeInt16(w, a.ErrorCode)
	writeArray(w, len(a.ApiVersions), func(i int) { a.ApiVersions[i].writeTo(w) })
}

func (a *apiVersionsResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &a.ErrorCode); err != nil {
		return
	}

	fn := func(withReader *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		var ver apiVersionsV0
		if fnRemain, fnErr = (&ver).readFrom(withReader, withSize); err != nil {
			return
		}
		a.ApiVersions = append(a.ApiVersions, ver)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

type apiVersionsV0 struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (a apiVersionsV0) size() int32 {
	return sizeofInt16(a.ApiKey) +
		sizeofInt16(a.MinVersion) +
		sizeofInt16(a.MaxVersion)
}

func (a apiVersionsV0) writeTo(w *bufio.Writer) {
	writeInt16(w, a.ApiKey)
	writeInt16(w, a.MinVersion)
	writeInt16(w, a.MaxVersion)
}

func (a *apiVersionsV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &a.ApiKey); err != nil {
		return
	}
	if remain, err = readInt16(r, size, &a.MinVersion); err != nil {
		return
	}
	if remain, err = readInt16(r, size, &a.MaxVersion); err != nil {
		return
	}

	return
}
