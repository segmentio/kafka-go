package kafka

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestMarshalUnmarshal(t *testing.T) {
	values := []interface{}{
		true,
		false,

		int8(0),
		int8(1),
		int8(math.MinInt8),
		int8(math.MaxInt8),

		int16(0),
		int16(1),
		int16(math.MinInt16),
		int16(math.MaxInt16),

		int32(0),
		int32(1),
		int32(math.MinInt32),
		int32(math.MaxInt32),

		int64(0),
		int64(1),
		int64(math.MinInt64),
		int64(math.MaxInt64),

		"",
		"hello world!",

		([]byte)(nil),
		[]byte(""),
		[]byte("hello world!"),

		([]int32)(nil),
		[]int32{},
		[]int32{0, 1, 2, 3, 4},

		struct{}{},
		struct {
			A int32
			B string
			C []byte
		}{A: 1, B: "42", C: []byte{}},
	}

	for _, v := range values {
		t.Run(fmt.Sprintf("%+v", v), func(t *testing.T) {
			b, err := Marshal(v)
			if err != nil {
				t.Fatal("marshal error:", err)
			}

			x := reflect.New(reflect.TypeOf(v))

			if err := Unmarshal(b, x.Interface()); err != nil {
				t.Fatal("unmarshal error:", err)
			}

			if !reflect.DeepEqual(v, x.Elem().Interface()) {
				t.Fatalf("values mismatch:\nexpected: %#v\nfound:   %#v\n", v, x.Elem().Interface())
			}
		})
	}
}

func TestVersionMarshalUnmarshal(t *testing.T) {
	type T struct {
		A int32  `kafka:"min=v0,max=v1"`
		B string `kafka:"min=v1,max=v2"`
		C []byte `kafka:"min=v2,max=v2,nullable"`
	}

	tests := []struct {
		out T
		ver Version
	}{
		{
			out: T{A: 42},
			ver: Version(0),
		},
	}

	in := T{
		A: 42,
		B: "Hello World!",
		C: []byte("question?"),
	}

	for _, test := range tests {
		t.Run(strconv.Itoa(int(test.ver)), func(t *testing.T) {
			b, err := test.ver.Marshal(in)
			if err != nil {
				t.Fatal("marshal error:", err)
			}

			x1 := test.out
			x2 := T{}

			if err := test.ver.Unmarshal(b, &x2); err != nil {
				t.Fatal("unmarshal error:", err)
			}

			if !reflect.DeepEqual(x1, x2) {
				t.Fatalf("values mismatch:\nexpected: %#v\nfound:   %#v\n", x1, x2)
			}
		})
	}

}

type Struct struct {
	A int32
	B int32
	C int32
}

var benchmarkValues = []interface{}{
	true,
	int8(1),
	int16(1),
	int32(1),
	int64(1),
	"Hello World!",
	[]byte("Hello World!"),
	[]int32{1, 2, 3},
	Struct{A: 1, B: 2, C: 3},
}

func BenchmarkMarshal(b *testing.B) {
	for _, v := range benchmarkValues {
		b.Run(fmt.Sprintf("%T", v), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := Marshal(v)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for _, v := range benchmarkValues {
		b.Run(fmt.Sprintf("%T", v), func(b *testing.B) {
			data, err := Marshal(v)

			if err != nil {
				b.Fatal(err)
			}

			value := reflect.New(reflect.TypeOf(v))
			ptr := value.Interface()
			elem := value.Elem()
			zero := reflect.Zero(reflect.TypeOf(v))

			for i := 0; i < b.N; i++ {
				if err := Unmarshal(data, ptr); err != nil {
					b.Fatal(err)
				}
				elem.Set(zero)
			}
		})
	}
}

type testKafkaLogger struct {
	Prefix string
	T      *testing.T
}

func newTestKafkaLogger(t *testing.T, prefix string) Logger {
	return &testKafkaLogger{Prefix: prefix, T: t}
}

func (l *testKafkaLogger) Printf(msg string, args ...interface{}) {
	l.T.Helper()
	if l.Prefix != "" {
		l.T.Logf(l.Prefix+" "+msg, args...)
	} else {
		l.T.Logf(msg, args...)
	}
}

type testRebalanceEventCallback struct {
	NoticeChan chan map[string][]PartitionAssignment
}

func newTestRebalanceEventCallback(c chan map[string][]PartitionAssignment) RebalanceEventInterceptor {
	return &testRebalanceEventCallback{NoticeChan: c}
}

func (c *testRebalanceEventCallback) Callback(partitionAssignments map[string][]PartitionAssignment) {
	c.NoticeChan <- partitionAssignments
}
