package protocol

import (
	"bufio"
	"fmt"
)

func ReadRequest(r *bufio.Reader) (apiVersion int16, correlationID int32, clientID string, msg Message, err error) {
	var size int32

	if size, err = readMessageSize(r); err != nil {
		return
	}

	fmt.Println("read request of size", size)

	d := &decoder{reader: r, remain: int(size)}
	defer d.discardAll()

	apiKey := d.readInt16()
	apiVersion = d.readInt16()
	correlationID = d.readInt32()
	clientID = d.readString()

	if i := int(apiKey); i < 0 || i >= len(apiTypes) {
		err = errorf("unsupported api key: %d", i)
		return
	}

	if d.err != nil {
		err = d.err
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

	req := &t.requests[apiVersion-minVersion]
	msg = req.new()
	req.decode(d, valueOf(msg))
	err = d.err
	return
}

func WriteRequest(w *bufio.Writer, apiVersion int16, correlationID int32, clientID string, msg Message) error {
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

	r := &t.requests[apiVersion-minVersion]
	v := valueOf(msg)
	b := newPageBuffer()
	defer b.unref()

	/*
		v1:
		00000000  00 00 00 9c 00 00 00 07  00 00 00 01 ff ff ff ff  |................|
		00000010  ff ff 00 00 0f a0 00 00  00 01 00 19 6b 61 66 6b  |............kafk|
		00000020  61 2d 67 6f 2d 31 63 36  61 62 35 63 63 61 37 63  |a-go-1c6ab5cca7c|
		00000030  31 62 35 33 32 00 00 00  01 00 00 00 00 00 00 00  |1b532...........|
		00000040  00 00 00 00 00 00 00 00  57 a0 f2 a8 f1 01 00 00  |........W.......|
		00000050  00 01 71 c9 e0 8f d1 ff  ff ff ff 00 00 00 07 68  |..q............h|
		00000060  65 6c 6c 6f 2d 31 b3 a2  5b 05 01 00 00 00 01 71  |ello-1..[......q|
		00000070  c9 e0 8f d1 ff ff ff ff  00 00 00 07 68 65 6c 6c  |............hell|
		00000080  6f 2d 32 41 c9 d8 06 01  00 00 00 01 71 c9 e0 8f  |o-2A........q...|
		00000090  d1 ff ff ff ff 00 00 00  07 68 65 6c 6c 6f 2d 33  |.........hello-3|

		v2:
		00000000  00 00 00 a0 00 00 00 07  00 00 00 01 ff ff ff ff  |................|
		00000010  ff ff 00 00 0f a0 00 00  00 01 00 19 6b 61 66 6b  |............kafk|
		00000020  61 2d 67 6f 2d 37 36 37  63 33 62 36 34 35 36 32  |a-go-767c3b64562|
		00000030  39 31 39 65 65 00 00 00  01 00 00 00 00 00 00 00  |919ee...........|
		00000040  00 00 00 00 00 00 00 00  5b ff ff ff ff 02 fb e9  |........[.......|
		00000050  d8 9f 00 00 00 00 00 02  00 00 01 71 c9 c8 fa 75  |...........q...u|
		00000060  00 00 01 71 c9 c8 fa 75  ff ff ff ff ff ff ff ff  |...q...u........|
		00000070  ff ff ff ff ff ff 00 00  00 03 18 00 00 00 01 0e  |................|
		00000080  68 65 6c 6c 6f 2d 31 00  18 00 00 02 01 0e 68 65  |hello-1.......he|
		00000090  6c 6c 6f 2d 32 00 18 00  00 04 01 0e 68 65 6c 6c  |llo-2.......hell|
		000000a0  6f 2d 33 00                                       |o-3.|

		conn (v2):
		00000000  00 00 00 ca 00 00 00 07  00 00 00 02 00 3d 6b 61  |.............=ka|
		00000010  66 6b 61 2d 67 6f 2e 74  65 73 74 40 61 63 68 69  |fka-go.test@achi|
		00000020  6c 6c 65 2e 72 6f 75 73  73 65 6c 20 28 67 69 74  |lle.roussel (git|
		00000030  68 75 62 2e 63 6f 6d 2f  73 65 67 6d 65 6e 74 69  |hub.com/segmenti|
		00000040  6f 2f 6b 61 66 6b 61 2d  67 6f 29 ff ff ff ff 7f  |o/kafka-go).....|
		00000050  ff ff ff 00 00 00 01 00  19 6b 61 66 6b 61 2d 67  |.........kafka-g|
		00000060  6f 2d 31 35 32 32 32 32  36 63 31 37 61 61 61 39  |o-1522226c17aaa9|
		00000070  62 33 00 00 00 01 00 00  00 00 00 00 00 50 00 00  |b3...........P..|
		00000080  00 00 00 00 00 00 00 00  00 44 ff ff ff ff 02 ca  |.........D......|
		00000090  ff f6 5d 00 00 00 00 00  00 00 00 01 71 c9 e1 21  |..].........q..!|
		000000a0  99 00 00 01 71 c9 e1 21  99 ff ff ff ff ff ff ff  |....q..!........|
		000000b0  ff ff ff ff ff ff ff 00  00 00 01 24 00 00 00 01  |...........$....|
		000000c0  18 48 65 6c 6c 6f 20 57  6f 72 6c 64 21 00        |.Hello World!.|
	*/

	e := &encoder{writer: b}
	e.writeInt32(0) // placeholder
	e.writeInt16(int16(apiKey))
	e.writeInt16(apiVersion)
	e.writeInt32(correlationID)
	e.writeNullString(clientID)
	r.encode(e, v)

	fmt.Println("write request of size", b.Size()-4)

	size := packUint32(uint32(b.Size()) - 4)
	b.WriteAt(size[:], 0)
	b.WriteTo(w)

	fmt.Println("done writing request", apiKey, apiVersion, correlationID)
	return w.Flush()
}
