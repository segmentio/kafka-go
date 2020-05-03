package protocol

import (
	"bufio"
)

func ReadResponse(r *bufio.Reader, apiKey, apiVersion int16) (correlationID int32, msg Message, err error) {
	var size int32

	if i := int(apiKey); i < 0 || i >= len(apiTypes) {
		err = errorf("unsupported api key: %d", i)
		return
	}

	t := &apiTypes[apiKey]
	if t == nil {
		err = errorf("unsupported api: %s", apiNames[apiKey])
		return
	}

	minVersion := t.minVersion()
	maxVersion := t.maxVersion()

	if apiVersion < minVersion || apiVersion > maxVersion {
		err = errorf("unsupported %s version: v%d not in range v%d-v%d",
			ApiKey(apiKey), apiVersion, minVersion, maxVersion)
		return
	}

	if size, err = readMessageSize(r); err != nil {
		return
	}

	d := &decoder{reader: r, remain: int(size)}
	defer d.discardAll()

	correlationID = d.readInt32()
	res := &t.responses[apiVersion-minVersion]
	msg = res.new()
	res.decode(d, valueOf(msg))
	err = dontExpectEOF(d.err)
	return
}

func WriteResponse(w *bufio.Writer, apiVersion int16, correlationID int32, msg Message) error {
	apiKey := msg.ApiKey()

	if i := int(apiKey); i < 0 || i >= len(apiTypes) {
		return errorf("unsupported api key: %d", i)
	}

	t := &apiTypes[apiKey]
	if t == nil {
		return errorf("unsupported api: %s", apiNames[apiKey])
	}

	minVersion := t.minVersion()
	maxVersion := t.maxVersion()

	if apiVersion < minVersion || apiVersion > maxVersion {
		return errorf("unsupported %s version: v%d not in range v%d-v%d",
			ApiKey(apiKey), apiVersion, minVersion, maxVersion)
	}

	r := &t.responses[apiVersion-minVersion]
	v := valueOf(msg)
	b := newPageBuffer()
	defer b.unref()

	e := &encoder{writer: b}
	e.writeInt32(0) // placeholder for the response size
	e.writeInt32(correlationID)
	r.encode(e, v)

	size := packUint32(uint32(b.Size()) - 4)
	b.WriteAt(size[:], 0)
	b.WriteTo(w)

	return w.Flush()
}
