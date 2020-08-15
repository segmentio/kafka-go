package protocol

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
)

func TestPageBufferWriteReadSeek(t *testing.T) {
	buffer := newPageBuffer()
	defer buffer.unref()

	io.WriteString(buffer, "Hello World!")

	if n := buffer.Size(); n != 12 {
		t.Fatal("invalid size:", n)
	}

	for i := 0; i < 3; i++ {
		if n := buffer.Len(); n != 12 {
			t.Fatal("invalid length before read:", n)
		}

		b, err := ioutil.ReadAll(buffer)
		if err != nil {
			t.Fatal(err)
		}

		if n := buffer.Len(); n != 0 {
			t.Fatal("invalid length after read:", n)
		}

		if string(b) != "Hello World!" {
			t.Fatalf("invalid content after read #%d: %q", i, b)
		}

		offset, err := buffer.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if offset != 0 {
			t.Fatalf("invalid offset after seek #%d: %d", i, offset)
		}
	}
}

func TestPageRefWriteReadSeek(t *testing.T) {
	buffer := newPageBuffer()
	defer buffer.unref()

	io.WriteString(buffer, "Hello World!")

	ref := buffer.ref(1, 11)
	defer ref.unref()

	if n := ref.Size(); n != 10 {
		t.Fatal("invalid size:", n)
	}

	for i := 0; i < 3; i++ {
		if n := ref.Len(); n != 10 {
			t.Fatal("invalid length before read:", n)
		}

		b, err := ioutil.ReadAll(ref)
		if err != nil {
			t.Fatal(err)
		}

		if n := ref.Len(); n != 0 {
			t.Fatal("invalid length after read:", n)
		}

		if string(b) != "ello World" {
			t.Fatalf("invalid content after read #%d: %q", i, b)
		}

		offset, err := ref.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if offset != 0 {
			t.Fatalf("invalid offset after seek #%d: %d", i, offset)
		}
	}
}

func TestPageRefReadByte(t *testing.T) {
	buffer := newPageBuffer()
	defer buffer.unref()

	content := bytes.Repeat([]byte("1234567890"), 10e3)
	buffer.Write(content)

	ref := buffer.ref(0, buffer.Size())
	defer ref.unref()

	for i, c := range content {
		b, err := ref.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		if b != c {
			t.Fatalf("byte at offset %d mismatch, expected '%c' but got '%c'", i, c, b)
		}
	}
}
