package kafka

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/compress/gzip"
	"github.com/segmentio/kafka-go/compress/lz4"
	"github.com/segmentio/kafka-go/compress/snappy"
	"github.com/segmentio/kafka-go/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestMessageSetReader(t *testing.T) {
	const startOffset = 1000
	const highWatermark = 5000
	const topic = "test-topic"
	msgs := make([]Message, 100)
	for i := 0; i < 100; i++ {
		msgs[i] = Message{
			Time:   time.Now(),
			Offset: int64(i + startOffset),
			Key:    []byte(fmt.Sprintf("key-%d", i)),
			Value:  []byte(fmt.Sprintf("val-%d", i)),
		}
	}
	defaultHeader := fetchResponseHeader{
		highWatermarkOffset: highWatermark,
		lastStableOffset:    highWatermark,
		topic:               topic,
	}
	for _, tc := range []struct {
		name    string
		builder fetchResponseBuilder
		err     error
		debug   bool
	}{
		{
			name: "empty",
			builder: fetchResponseBuilder{
				header: defaultHeader,
			},
			err: errShortRead,
		},
		{
			name: "v0",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v0MessageSetBuilder{
						Message: msgs[0],
					},
				},
			},
		},
		{
			name: "v0 compressed",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v0MessageSetBuilder{
						codec:   new(gzip.Codec),
						Message: msgs[0],
					},
				},
			},
		},
		{
			name: "v1",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v1MessageSetBuilder{
						Message: msgs[0],
					},
				},
			},
		},
		{
			name: "v1 compressed",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v1MessageSetBuilder{
						codec:   new(gzip.Codec),
						Message: msgs[0],
					},
				},
			},
		},
		{
			name: "v2",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v2MessageSetBuilder{
						msgs: []Message{msgs[0]},
					},
				},
			},
		},
		{
			name: "v2 compressed",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v2MessageSetBuilder{
						codec: new(zstd.Codec),
						msgs:  []Message{msgs[0]},
					},
				},
			},
		},
		{
			name: "v2 multiple messages",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v2MessageSetBuilder{
						msgs: []Message{msgs[0], msgs[1], msgs[2], msgs[3], msgs[4]},
					},
				},
			},
		},
		{
			name: "v2 multiple messages compressed",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v2MessageSetBuilder{
						codec: new(snappy.Codec),
						msgs:  []Message{msgs[0], msgs[1], msgs[2], msgs[3], msgs[4]},
					},
				},
			},
		},
		{
			name: "v2 mix of compressed and uncompressed message sets",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v2MessageSetBuilder{
						codec: new(snappy.Codec),
						msgs:  []Message{msgs[0], msgs[1], msgs[2], msgs[3], msgs[4]},
					},
					v2MessageSetBuilder{
						msgs: []Message{msgs[5], msgs[6], msgs[7], msgs[8], msgs[9]},
					},
					v2MessageSetBuilder{
						codec: new(snappy.Codec),
						msgs:  []Message{msgs[10], msgs[11], msgs[12], msgs[13], msgs[14]},
					},
					v2MessageSetBuilder{
						msgs: []Message{msgs[15], msgs[16], msgs[17], msgs[18], msgs[19]},
					},
				},
			},
		},
		{
			name: "v0 v2 v1 v2 v1 v1 v0 v2",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v0MessageSetBuilder{
						Message: msgs[0],
					},
					v2MessageSetBuilder{
						msgs: []Message{msgs[1], msgs[2]},
					},
					v1MessageSetBuilder{
						Message: msgs[3],
					},
					v2MessageSetBuilder{
						msgs: []Message{msgs[4], msgs[5]},
					},
					v1MessageSetBuilder{
						Message: msgs[6],
					},
					v1MessageSetBuilder{
						Message: msgs[7],
					},
					v0MessageSetBuilder{
						Message: msgs[8],
					},
					v2MessageSetBuilder{
						msgs: []Message{msgs[9], msgs[10]},
					},
				},
			},
		},
		{
			name: "v0 v2 v1 v2 v1 v1 v0 v2 mixed compression",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v0MessageSetBuilder{
						codec:   new(gzip.Codec),
						Message: msgs[0],
					},
					v2MessageSetBuilder{
						codec: new(zstd.Codec),
						msgs:  []Message{msgs[1], msgs[2]},
					},
					v1MessageSetBuilder{
						codec:   new(snappy.Codec),
						Message: msgs[3],
					},
					v2MessageSetBuilder{
						codec: new(lz4.Codec),
						msgs:  []Message{msgs[4], msgs[5]},
					},
					v1MessageSetBuilder{
						codec:   new(gzip.Codec),
						Message: msgs[6],
					},
					v1MessageSetBuilder{
						codec:   new(zstd.Codec),
						Message: msgs[7],
					},
					v0MessageSetBuilder{
						codec:   new(snappy.Codec),
						Message: msgs[8],
					},
					v2MessageSetBuilder{
						codec: new(lz4.Codec),
						msgs:  []Message{msgs[9], msgs[10]},
					},
				},
			},
		},
		{
			name: "v0 v2 v1 v2 v1 v1 v0 v2 mixed compression with non-compressed",
			builder: fetchResponseBuilder{
				header: defaultHeader,
				msgSets: []messageSetBuilder{
					v0MessageSetBuilder{
						codec:   new(gzip.Codec),
						Message: msgs[0],
					},
					v2MessageSetBuilder{
						msgs: []Message{msgs[1], msgs[2]},
					},
					v1MessageSetBuilder{
						codec:   new(snappy.Codec),
						Message: msgs[3],
					},
					v2MessageSetBuilder{
						msgs: []Message{msgs[4], msgs[5]},
					},
					v1MessageSetBuilder{
						Message: msgs[6],
					},
					v1MessageSetBuilder{
						codec:   new(zstd.Codec),
						Message: msgs[7],
					},
					v0MessageSetBuilder{
						Message: msgs[8],
					},
					v2MessageSetBuilder{
						codec: new(lz4.Codec),
						msgs:  []Message{msgs[9], msgs[10]},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rh, err := newReaderHelper(t, tc.builder.bytes())
			require.Equal(t, tc.err, err)
			if tc.err != nil {
				return
			}
			rh.offset = tc.builder.messages()[0].Offset
			rh.debug = tc.debug
			for _, expected := range tc.builder.messages() {
				msg := rh.readMessage()
				require.Equal(t, string(expected.Key), string(msg.Key))
				require.Equal(t, string(expected.Value), string(msg.Value))
				require.Equal(t, expected.Offset, msg.Offset)
			}
			// verify the reader stack is empty
			require.EqualValues(t, 0, rh.remain)
			require.EqualValues(t, 0, rh.count)
			require.EqualValues(t, 0, rh.remaining())
			require.Nil(t, rh.readerStack.parent)
			// any further message is a short read
			_, err = rh.readMessageErr()
			require.EqualError(t, err, errShortRead.Error())
		})
	}

}

func TestMessageSetReaderEmpty(t *testing.T) {
	m := messageSetReader{empty: true}

	noop := func(*bufio.Reader, int, int) (int, error) {
		return 0, nil
	}

	offset, timestamp, headers, err := m.readMessage(0, noop, noop)
	if offset != 0 {
		t.Errorf("expected offset of 0, get %d", offset)
	}
	if timestamp != 0 {
		t.Errorf("expected timestamp of 0, get %d", timestamp)
	}
	if headers != nil {
		t.Errorf("expected nil headers, got %v", headers)
	}
	if err != RequestTimedOut {
		t.Errorf("expected RequestTimedOut, got %v", err)
	}

	if m.remaining() != 0 {
		t.Errorf("expected 0 remaining, got %d", m.remaining())
	}

	if m.discard() != nil {
		t.Errorf("unexpected error from discard(): %v", m.discard())
	}
}

func TestMessageFixtures(t *testing.T) {
	type fixtureMessage struct {
		key   string
		value string
	}
	var fixtureMessages = map[string]fixtureMessage{
		"a": {key: "alpha", value: `{"count":0,"filler":"aaaaaaaaaa"}`},
		"b": {key: "beta", value: `{"count":0,"filler":"bbbbbbbbbb"}`},
		"c": {key: "gamma", value: `{"count":0,"filler":"cccccccccc"}`},
		"d": {key: "delta", value: `{"count":0,"filler":"dddddddddd"}`},
		"e": {key: "epsilon", value: `{"count":0,"filler":"eeeeeeeeee"}`},
		"f": {key: "zeta", value: `{"count":0,"filler":"ffffffffff"}`},
		"g": {key: "eta", value: `{"count":0,"filler":"gggggggggg"}`},
		"h": {key: "theta", value: `{"count":0,"filler":"hhhhhhhhhh"}`},
	}

	for _, tc := range []struct {
		name     string
		file     string
		messages []string
	}{
		{
			name:     "v2 followed by v1",
			file:     "fixtures/v2b-v1.hex",
			messages: []string{"a", "b", "a", "b"},
		},
		{
			name:     "v2 compressed followed by v1 compressed",
			file:     "fixtures/v2bc-v1c.hex",
			messages: []string{"a", "b", "a", "b"},
		},
		{
			name:     "v2 compressed followed by v1 uncompressed",
			file:     "fixtures/v2bc-v1.hex",
			messages: []string{"a", "b", "c", "d"},
		},
		{
			name:     "v2 compressed followed by v1 uncompressed then v1 compressed",
			file:     "fixtures/v2bc-v1-v1c.hex",
			messages: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:     "v2 compressed followed by v1 uncompressed then v1 compressed",
			file:     "fixtures/v2bc-v1-v1c.hex",
			messages: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:     "v1 followed by v1",
			file:     "fixtures/v1-v1.hex",
			messages: []string{"a", "b", "c", "d"},
		},
		{
			name:     "v1 compressed followed by v1 compressed",
			file:     "fixtures/v1c-v1c.hex",
			messages: []string{"a", "b", "c", "d"},
		},
		{
			name:     "v1 compressed followed by v1 uncompressed then v1 compressed",
			file:     "fixtures/v1c-v1-v1c.hex",
			messages: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:     "v2 followed by v2",
			file:     "fixtures/v2-v2.hex",
			messages: []string{"a", "b", "c", "d"},
		},
		{
			name:     "v2 compressed followed by v2 compressed",
			file:     "fixtures/v2c-v2c.hex",
			messages: []string{"a", "b", "c", "d"},
		},
		{
			name:     "v2 compressed followed by v2 uncompressed then v2 compressed",
			file:     "fixtures/v2c-v2-v2c.hex",
			messages: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:     "v1 followed by v2 followed by v1 with mixture of compressed and uncompressed",
			file:     "fixtures/v1-v1c-v2-v2c-v2b-v2b-v2b-v2bc-v1b-v1bc.hex",
			messages: []string{"a", "b", "a", "b", "c", "d", "c", "d", "e", "f", "e", "f", "g", "h", "g", "h", "g", "h", "g", "h"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bs, err := os.ReadFile(tc.file)
			require.NoError(t, err)
			buf := new(bytes.Buffer)
			_, err = io.Copy(buf, hex.NewDecoder(bytes.NewReader(bs)))
			require.NoError(t, err)

			// discard 4 byte len and 4 byte correlation id
			bs = make([]byte, 8)
			buf.Read(bs)

			rh, err := newReaderHelper(t, buf.Bytes())
			require.NoError(t, err)
			messagesCount := 0
			expectedMessageCount := len(tc.messages)
			for _, expectedMessageId := range tc.messages {
				expectedMessage := fixtureMessages[expectedMessageId]
				msg := rh.readMessage()
				messagesCount++
				require.Equal(t, expectedMessage.key, string(msg.Key))
				require.Equal(t, expectedMessage.value, string(msg.Value))
				t.Logf("Message %d key & value are what we expected: %s -> %s\n",
					messagesCount, string(msg.Key), string(msg.Value))
			}
			require.Equal(t, expectedMessageCount, messagesCount)
		})
	}
}

func TestMessageSize(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 20; i++ {
		t.Run("Run", func(t *testing.T) {
			msg := Message{
				Key:   make([]byte, rand.Intn(200)),
				Value: make([]byte, rand.Intn(200)),
				Time:  randate(),
			}
			expSize := msg.message(nil).size()
			gotSize := msg.size()
			if expSize != gotSize {
				t.Errorf("Expected size %d, but got size %d", expSize, gotSize)
			}
		})
	}

}

// https://stackoverflow.com/questions/43495745/how-to-generate-random-date-in-go-lang/43497333#43497333
func randate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// readerHelper composes a messageSetReader to provide convenience methods to read
// messages.
type readerHelper struct {
	t *testing.T
	*messageSetReader
	offset int64
}

func newReaderHelper(t *testing.T, bs []byte) (r *readerHelper, err error) {
	bufReader := bufio.NewReader(bytes.NewReader(bs))
	_, _, remain, err := readFetchResponseHeaderV10(bufReader, len(bs))
	require.NoError(t, err)
	var msgs *messageSetReader
	msgs, err = newMessageSetReader(bufReader, remain)
	if err != nil {
		return
	}
	r = &readerHelper{t: t, messageSetReader: msgs}
	require.Truef(t, msgs.remaining() > 0, "remaining should be > 0 but was %d", msgs.remaining())
	return
}

func (r *readerHelper) readMessageErr() (msg Message, err error) {
	keyFunc := func(r *bufio.Reader, size int, nbytes int) (remain int, err error) {
		msg.Key, remain, err = readNewBytes(r, size, nbytes)
		return
	}
	valueFunc := func(r *bufio.Reader, size int, nbytes int) (remain int, err error) {
		msg.Value, remain, err = readNewBytes(r, size, nbytes)
		return
	}
	var timestamp int64
	var headers []Header
	r.offset, timestamp, headers, err = r.messageSetReader.readMessage(r.offset, keyFunc, valueFunc)
	if err != nil {
		return
	}
	msg.Offset = r.offset
	msg.Time = time.UnixMilli(timestamp)
	msg.Headers = headers
	return
}

func (r *readerHelper) readMessage() (msg Message) {
	var err error
	msg, err = r.readMessageErr()
	require.NoError(r.t, err)
	return
}
