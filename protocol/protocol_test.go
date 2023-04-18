package protocol

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

type testType struct {
	Field1   string        `kafka:"min=v0,max=v4,nullable"`
	Field2   int16         `kafka:"min=v2,max=v4"`
	Field3   []byte        `kafka:"min=v2,max=v4,nullable"`
	SubTypes []testSubType `kafka:"min=v1,max=v4"`

	TaggedField1 int8   `kafka:"min=v3,max=v4,tag=0"`
	TaggedField2 string `kafka:"min=v4,max=v4,tag=1"`
}

type testSubType struct {
	SubField1 int8 `kafka:"min=v1,max=v4"`
}

func TestMakeFlexibleTypes(t *testing.T) {
	types := makeTypes(reflect.TypeOf(&testType{}).Elem())
	if len(types) != 5 {
		t.Error(
			"Wrong number of types",
			"expected", 5,
			"got", len(types),
		)
	}

	fv := []int16{}

	for _, to := range types {
		if to.flexible {
			fv = append(fv, to.version)
		}
	}

	if !reflect.DeepEqual([]int16{3, 4}, fv) {
		t.Error(
			"Unexpected flexible versions",
			"expected", []int16{3, 4},
			"got", fv,
		)
	}
}

func TestEncodeDecodeFlexibleType(t *testing.T) {
	f := &testType{
		Field1: "value1",
		Field2: 15,
		Field3: []byte("hello"),
		SubTypes: []testSubType{
			{
				SubField1: 2,
			},
			{
				SubField1: 3,
			},
		},

		TaggedField1: 34,
		TaggedField2: "taggedValue2",
	}

	b := &bytes.Buffer{}
	e := &encoder{writer: b}

	types := makeTypes(reflect.TypeOf(&testType{}).Elem())
	ft := types[4]
	ft.encode(e, valueOf(f))
	if e.err != nil {
		t.Error(
			"Error during encoding",
			"expected", nil,
			"got", e.err,
		)
	}

	exp := []byte{
		// size of "value1" + 1
		7,
		// "value1"
		118, 97, 108, 117, 101, 49,
		// 15 as 16-bit int
		0, 15,
		// size of []byte("hello") + 1
		6,
		// []byte("hello")
		104, 101, 108, 108, 111,
		// size of []SubTypes + 1
		3,
		// 2 as 8-bit int
		2,
		// tag buffer for first SubType struct
		0,
		// 3 as 8-bit int
		3,
		// tag buffer for second SubType struct
		0,
		// number of tagged fields
		2,
		// id of first tagged field
		0,
		// size of first tagged field
		1,
		// 34 as 8-bit int
		34,
		// id of second tagged field
		1,
		// size of second tagged field
		13,
		// size of "taggedValue2" + 1
		13,
		// "taggedValue2"
		116, 97, 103, 103, 101, 100, 86, 97, 108, 117, 101, 50,
	}

	if !reflect.DeepEqual(exp, b.Bytes()) {
		t.Error(
			"Wrong encoded output",
			"expected", exp,
			"got", b.Bytes(),
		)
	}

	b = &bytes.Buffer{}
	b.Write(exp)
	d := &decoder{reader: b, remain: len(exp)}

	f2 := &testType{}
	ft.decode(d, valueOf(f2))
	if d.err != nil {
		t.Error(
			"Error during decoding",
			"expected", nil,
			"got", e.err,
		)
	}

	if !reflect.DeepEqual(f, f2) {
		t.Error(
			"Decoded value does not equal encoded one",
			"expected", *f,
			"got", *f2,
		)
	}
}

func TestVarInts(t *testing.T) {
	type tc struct {
		input      int64
		expVarInt  []byte
		expUVarInt []byte
	}

	tcs := []tc{
		{
			input:      12,
			expVarInt:  []byte{24},
			expUVarInt: []byte{12},
		},
		{
			input:      63,
			expVarInt:  []byte{126},
			expUVarInt: []byte{63},
		},
		{
			input:      -64,
			expVarInt:  []byte{127},
			expUVarInt: []byte{192, 255, 255, 255, 255, 255, 255, 255, 255, 1},
		},
		{
			input:      64,
			expVarInt:  []byte{128, 1},
			expUVarInt: []byte{64},
		},
		{
			input:      127,
			expVarInt:  []byte{254, 1},
			expUVarInt: []byte{127},
		},
		{
			input:      128,
			expVarInt:  []byte{128, 2},
			expUVarInt: []byte{128, 1},
		},
		{
			input:      129,
			expVarInt:  []byte{130, 2},
			expUVarInt: []byte{129, 1},
		},
		{
			input:      12345,
			expVarInt:  []byte{242, 192, 1},
			expUVarInt: []byte{185, 96},
		},
		{
			input:      123456789101112,
			expVarInt:  []byte{240, 232, 249, 224, 144, 146, 56},
			expUVarInt: []byte{184, 244, 188, 176, 136, 137, 28},
		},
	}

	for _, tc := range tcs {
		b := &bytes.Buffer{}
		e := &encoder{writer: b}
		e.writeVarInt(tc.input)
		if e.err != nil {
			t.Errorf(
				"Unexpected error encoding %d as varInt: %+v",
				tc.input,
				e.err,
			)
		}
		if !reflect.DeepEqual(b.Bytes(), tc.expVarInt) {
			t.Error(
				"Wrong output encoding value", tc.input, "as varInt",
				"expected", tc.expVarInt,
				"got", b.Bytes(),
			)
		}
		expLen := sizeOfVarInt(tc.input)
		if expLen != len(b.Bytes()) {
			t.Error(
				"Wrong sizeOf for", tc.input, "as varInt",
				"expected", expLen,
				"got", len(b.Bytes()),
			)
		}

		d := &decoder{reader: b, remain: len(b.Bytes())}
		v := d.readVarInt()
		if v != tc.input {
			t.Error(
				"Decoded varInt value does not equal encoded one",
				"expected", tc.input,
				"got", v,
			)
		}

		b = &bytes.Buffer{}
		e = &encoder{writer: b}
		e.writeUnsignedVarInt(uint64(tc.input))
		if e.err != nil {
			t.Errorf(
				"Unexpected error encoding %d as unsignedVarInt: %+v",
				tc.input,
				e.err,
			)
		}
		if !reflect.DeepEqual(b.Bytes(), tc.expUVarInt) {
			t.Error(
				"Wrong output encoding value", tc.input, "as unsignedVarInt",
				"expected", tc.expUVarInt,
				"got", b.Bytes(),
			)
		}
		expLen = sizeOfUnsignedVarInt(uint64(tc.input))
		if expLen != len(b.Bytes()) {
			t.Error(
				"Wrong sizeOf for", tc.input, "as unsignedVarInt",
				"expected", expLen,
				"got", len(b.Bytes()),
			)
		}

		d = &decoder{reader: b, remain: len(b.Bytes())}
		v = int64(d.readUnsignedVarInt())
		if v != tc.input {
			t.Error(
				"Decoded unsignedVarInt value does not equal encoded one",
				"expected", tc.input,
				"got", v,
			)
		}

	}
}

func TestFloat64(t *testing.T) {
	type tc struct {
		input    float64
		expected []byte
	}

	tcs := []tc{
		{
			input:    0.0,
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			input:    math.MaxFloat64,
			expected: []byte{127, 239, 255, 255, 255, 255, 255, 255},
		},
		{
			input:    -math.MaxFloat64,
			expected: []byte{255, 239, 255, 255, 255, 255, 255, 255},
		},
		{
			input:    math.SmallestNonzeroFloat64,
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			input:    -math.SmallestNonzeroFloat64,
			expected: []byte{128, 0, 0, 0, 0, 0, 0, 1},
		},
	}

	for _, tc := range tcs {
		b := &bytes.Buffer{}
		e := &encoder{writer: b}
		e.writeFloat64(tc.input)
		if e.err != nil {
			t.Errorf(
				"Unexpected error encoding %f as float64: %+v",
				tc.input,
				e.err,
			)
		}
		if !reflect.DeepEqual(b.Bytes(), tc.expected) {
			t.Error(
				"Wrong output encoding value", tc.input, "as float64",
				"expected", tc.expected,
				"got", b.Bytes(),
			)
		}

		d := &decoder{reader: b, remain: len(b.Bytes())}
		v := d.readFloat64()
		if v != tc.input {
			t.Error(
				"Decoded float64 value does not equal encoded one",
				"expected", tc.input,
				"got", v,
			)
		}
	}
}
