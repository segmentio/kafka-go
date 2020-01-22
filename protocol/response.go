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
	err = d.err
	return
}

func WriteResponse(w *bufio.Writer, apiVersion int16, correlationID int32, msg Message) error {
	apiKey := msg.ApiKey()

	if i := int(apiKey); i < 0 || i >= len(apiTypes) {
		return errorf("unsupported api key: %d", i)
	}

	t := &apiTypes[apiKey]
	minVersion := t.minVersion()
	maxVersion := t.maxVersion()

	if apiVersion < minVersion || apiVersion > maxVersion {
		return errorf("unsupported %s version: v%d not in range v%d-v%d",
			ApiKey(apiKey), apiVersion, minVersion, maxVersion)
	}

	const (
		sizeOfCorrelationID = 4
	)

	r := &t.responses[apiVersion-minVersion]
	v := valueOf(msg)
	n := sizeOfCorrelationID + r.size(v)

	e := &encoder{writer: w}
	e.writeInt32(int32(n))
	e.writeInt32(correlationID)
	r.encode(e, v)
	return w.Flush()
}
