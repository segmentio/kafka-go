package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// ByteSequence is an interface implemented by types that represent immutable
// sequences of bytes.
//
// ByteSequence values are used to abstract the location where record keys and
// values are read from (e.g. in-memory buffers, network sockets, files).
//
// The Close method should be called to release resources held by the sequence
// when the program is done with it.
//
// ByteSequence values are generally not safe to use concurrently from multiple
// goroutines.
type ByteSequence interface {
	Size() int64
	io.Closer
	io.Seeker
	io.Reader
	io.ReaderAt
}

// Bytes constructs a ByteSequence which exposes the content of b.
func Bytes(b []byte) ByteSequence {
	r := &bytesReader{}
	r.Reset(b)
	return r
}

type bytesReader struct{ bytes.Reader }

func (r *bytesReader) Close() error { r.Reset(nil); return nil }

// String constructs a ByteSequence which exposes the content of s.
func String(s string) ByteSequence {
	r := &stringReader{}
	r.Reset(s)
	return r
}

type stringReader struct{ strings.Reader }

func (r *stringReader) Close() error { r.Reset(""); return nil }

type refCount uintptr

func (rc *refCount) ref() { atomic.AddUintptr((*uintptr)(rc), 1) }

func (rc *refCount) unref(onZero func()) {
	if atomic.AddUintptr((*uintptr)(rc), ^uintptr(0)) == 0 {
		onZero()
	}
}

const (
	// Size of the memory buffer for a single page. We use a farily
	// large size here (64 KiB) because batches exchanged with kafka
	// tend to be multiple kilobytes in size, sometimes hundreds.
	// Using large pages amortizes the overhead of the page metadata
	// and algorithms to manage the pages.
	pageSize = 65536
)

type page struct {
	refc   refCount
	offset int64
	length int
	buffer *[pageSize]byte
}

func newPage(offset int64) *page {
	p := pagePool.Get().(*page)
	p.offset = offset
	p.length = 0
	p.ref()
	return p
}

func (p *page) ref() { p.refc.ref() }

func (p *page) unref() { p.refc.unref(func() { pagePool.Put(p) }) }

func (p *page) slice(begin, end int64) []byte {
	i, j := begin-p.offset, end-p.offset

	if i < 0 {
		i = 0
	} else if i > pageSize {
		i = pageSize
	}

	if j < 0 {
		j = 0
	} else if j > pageSize {
		j = pageSize
	}

	if i < j {
		return p.buffer[i:j]
	}

	return nil
}

func (p *page) Cap() int { return pageSize }

func (p *page) Len() int { return p.length }

func (p *page) Size() int64 { return int64(p.length) }

func (p *page) Truncate(n int) {
	if n < p.length {
		p.length = n
	}
}

func (p *page) ReadAt(b []byte, off int64) (int, error) {
	if off -= p.offset; off < 0 || off > pageSize {
		panic("offset out of range")
	}
	if off > int64(p.length) {
		return 0, nil
	}
	return copy(b, p.buffer[off:p.length]), nil
}

func (p *page) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.ReadFull(r, p.buffer[p.length:])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		err = nil
	}
	p.length += n
	return int64(n), err
}

func (p *page) WriteAt(b []byte, off int64) (int, error) {
	if off -= p.offset; off < 0 || off > pageSize {
		panic("offset out of range")
	}
	n := copy(p.buffer[off:], b)
	if end := int(off) + n; end > p.length {
		p.length = end
	}
	return n, nil
}

func (p *page) Write(b []byte) (int, error) {
	return p.WriteAt(b, p.offset+int64(p.length))
}

var pagePool = sync.Pool{
	New: func() interface{} {
		return &page{buffer: &[pageSize]byte{}}
	},
}

var (
	_ io.ReaderAt   = (*page)(nil)
	_ io.ReaderFrom = (*page)(nil)
	_ io.Writer     = (*page)(nil)
	_ io.WriterAt   = (*page)(nil)
)

type pageBuffer struct {
	refc   refCount
	pages  contiguousPages
	length int
	cursor int
}

func newPageBuffer() *pageBuffer {
	b := pageBufferPool.Get().(*pageBuffer)
	b.cursor = 0
	b.refc.ref()
	return b
}

func (pb *pageBuffer) ref(begin, end int64) *pageRef {
	pb.refc.ref()
	return &pageRef{
		buffer: pb,
		pages:  pb.pages.slice(begin, end),
		offset: begin,
		length: int(end - begin),
	}
}

func (pb *pageBuffer) unref() {
	pb.refc.unref(func() {
		for _, p := range pb.pages {
			p.unref()
		}
		for i := range pb.pages {
			pb.pages[i] = nil
		}
		pb.pages = pb.pages[:0]
		pb.length = 0
		pageBufferPool.Put(pb)
	})
}

func (pb *pageBuffer) newPage() *page {
	return newPage(int64(pb.length))
}

func (pb *pageBuffer) Close() error {
	return nil
}

func (pb *pageBuffer) Len() int {
	return pb.length
}

func (pb *pageBuffer) Size() int64 {
	return int64(pb.length)
}

func (pb *pageBuffer) Discard(n int) (int, error) {
	remain := pb.length - pb.cursor
	if remain < n {
		n = remain
	}
	pb.cursor += n
	return n, nil
}

func (pb *pageBuffer) Truncate(n int) {
	if n < pb.length {
		pb.length = n

		if n < pb.cursor {
			pb.cursor = n
		}

		for i := range pb.pages {
			if p := pb.pages[i]; p.length <= n {
				n -= p.length
			} else {
				if n > 0 {
					pb.pages[i].Truncate(n)
					i++
				}
				for j := i; j < len(pb.pages); j++ {
					pb.pages[j].unref()
				}
				for j := i; j < len(pb.pages); j++ {
					pb.pages[j] = nil
				}
				break
			}
		}
	}
}

func (pb *pageBuffer) Seek(offset int64, whence int) (int64, error) {
	c, err := seek(int64(pb.cursor), int64(pb.length), offset, whence)
	if err != nil {
		return -1, err
	}
	pb.cursor = int(c)
	return c, nil
}

func (pb *pageBuffer) ReadByte() (byte, error) {
	b := [1]byte{}
	_, err := pb.Read(b[:])
	return b[0], err
}

func (pb *pageBuffer) Read(b []byte) (int, error) {
	if pb.cursor >= pb.length {
		return 0, io.EOF
	}
	n, err := pb.ReadAt(b, int64(pb.cursor))
	pb.cursor += n
	return n, err
}

func (pb *pageBuffer) ReadAt(b []byte, off int64) (int, error) {
	return pb.pages.ReadAt(b, off)
}

func (pb *pageBuffer) ReadFrom(r io.Reader) (int64, error) {
	if len(pb.pages) == 0 {
		pb.pages = append(pb.pages, pb.newPage())
	}

	rn := int64(0)

	for {
		tail := pb.pages[len(pb.pages)-1]
		free := tail.Cap() - tail.Len()

		if free == 0 {
			tail = pb.newPage()
			free = pageSize
			pb.pages = append(pb.pages, tail)
		}

		n, err := tail.ReadFrom(r)
		pb.length += int(n)
		rn += n
		if n < int64(free) {
			return rn, err
		}
	}
}

func (pb *pageBuffer) WriteString(s string) (int, error) {
	return pb.Write([]byte(s))
}

func (pb *pageBuffer) Write(b []byte) (int, error) {
	wn := len(b)
	if wn == 0 {
		return 0, nil
	}

	if len(pb.pages) == 0 {
		pb.pages = append(pb.pages, pb.newPage())
	}

	for len(b) != 0 {
		tail := pb.pages[len(pb.pages)-1]
		free := tail.Cap() - tail.Len()

		if len(b) <= free {
			tail.Write(b)
			pb.length += len(b)
			break
		}

		tail.Write(b[:free])
		b = b[free:]

		pb.length += free
		pb.pages = append(pb.pages, pb.newPage())
	}

	return wn, nil
}

func (pb *pageBuffer) WriteAt(b []byte, off int64) (int, error) {
	n, err := pb.pages.WriteAt(b, off)
	if err != nil {
		return n, err
	}
	if n < len(b) {
		pb.Write(b[n:])
	}
	return len(b), nil
}

func (pb *pageBuffer) WriteTo(w io.Writer) (int64, error) {
	var wn int
	var err error
	pb.pages.scan(int64(pb.cursor), int64(pb.length), func(b []byte) bool {
		var n int
		n, err = w.Write(b)
		wn += n
		return err == nil
	})
	pb.cursor += wn
	return int64(wn), err
}

var (
	_ io.ReaderAt     = (*pageBuffer)(nil)
	_ io.ReaderFrom   = (*pageBuffer)(nil)
	_ io.StringWriter = (*pageBuffer)(nil)
	_ io.Writer       = (*pageBuffer)(nil)
	_ io.WriterAt     = (*pageBuffer)(nil)
	_ io.WriterTo     = (*pageBuffer)(nil)

	pageBufferPool = sync.Pool{
		New: func() interface{} {
			return &pageBuffer{pages: make(contiguousPages, 0, 16)}
		},
	}
)

type contiguousPages []*page

func (pages contiguousPages) ReadAt(b []byte, off int64) (int, error) {
	rn := 0

	for _, p := range pages.slice(off, off+int64(len(b))) {
		n, _ := p.ReadAt(b, off)
		b = b[n:]
		rn += n
		off += int64(n)
	}

	return rn, nil
}

func (pages contiguousPages) WriteAt(b []byte, off int64) (int, error) {
	wn := 0

	for _, p := range pages.slice(off, off+int64(len(b))) {
		n, _ := p.WriteAt(b, off)
		b = b[n:]
		wn += n
		off += int64(n)
	}

	return wn, nil
}

func (pages contiguousPages) slice(begin, end int64) contiguousPages {
	i := pages.indexOf(begin)
	j := pages.indexOf(end)
	if j < len(pages) {
		j++
	}
	return pages[i:j]
}

func (pages contiguousPages) indexOf(offset int64) int {
	return sort.Search(len(pages), func(i int) bool {
		return offset < (pages[i].offset + pages[i].Size())
	})
}

func (pages contiguousPages) scan(begin, end int64, f func([]byte) bool) {
	for _, p := range pages.slice(begin, end) {
		if !f(p.slice(begin, end)) {
			break
		}
	}
}

var (
	_ io.ReaderAt = contiguousPages{}
	_ io.WriterAt = contiguousPages{}
)

type pageRef struct {
	buffer *pageBuffer
	pages  contiguousPages
	offset int64
	cursor int64
	length int
	once   sync.Once
}

func (ref *pageRef) unref() {
	var buffer *pageBuffer
	ref.once.Do(func() {
		buffer = ref.buffer
		ref.buffer = nil
		ref.pages = nil
		ref.offset = 0
		ref.cursor = 0
		ref.length = 0
	})
	if buffer != nil {
		buffer.unref()
	}
}

func (ref *pageRef) Size() int64 { return int64(ref.length) }

func (ref *pageRef) Close() error { ref.unref(); return nil }

func (ref *pageRef) String() string {
	return fmt.Sprintf("[offset=%d cursor=%d length=%d]", ref.offset, ref.cursor, ref.length)
}

func (ref *pageRef) Seek(offset int64, whence int) (int64, error) {
	c, err := seek(ref.cursor, int64(ref.length), offset, whence)
	if err != nil {
		return -1, err
	}
	ref.cursor = c
	return c, nil
}

func (ref *pageRef) Read(b []byte) (int, error) {
	if ref.cursor >= int64(ref.length) {
		return 0, io.EOF
	}
	n, err := ref.ReadAt(b, ref.cursor)
	ref.cursor += int64(n)
	return n, err
}

func (ref *pageRef) ReadAt(b []byte, off int64) (n int, err error) {
	limit := ref.offset + int64(ref.length)

	switch off += ref.offset; {
	case off >= limit:
		b, off = nil, limit
	case off+int64(len(b)) > limit:
		b = b[:limit-off]
	}

	if len(b) != 0 {
		n, err = ref.pages.ReadAt(b, off)
		if n == 0 && err == nil {
			err = io.EOF
		}
	}

	return
}

func (ref *pageRef) WriteTo(w io.Writer) (wn int64, err error) {
	ref.scan(ref.cursor, func(b []byte) bool {
		var n int
		n, err = w.Write(b)
		wn += int64(n)
		return err == nil
	})
	ref.cursor += wn
	return
}

func (ref *pageRef) scan(off int64, f func([]byte) bool) {
	begin := ref.offset + off
	end := ref.offset + int64(ref.length)

	if begin < end {
		for _, p := range ref.pages {
			if !f(p.slice(begin, end)) {
				break
			}
		}
	}
}

var (
	_ io.Closer   = (*pageRef)(nil)
	_ io.Reader   = (*pageRef)(nil)
	_ io.ReaderAt = (*pageRef)(nil)
	_ io.WriterTo = (*pageRef)(nil)
)

func unref(x interface{}) {
	if r, _ := x.(interface{ unref() }); r != nil {
		r.unref()
	}
}

func seek(cursor, limit, offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// absolute offset
	case io.SeekCurrent:
		offset = cursor + offset
	case io.SeekEnd:
		offset = limit - offset
	default:
		return -1, fmt.Errorf("seek: invalid whence value: %d", whence)
	}
	if offset < 0 {
		offset = 0
	}
	if offset > limit {
		offset = limit
	}
	return offset, nil
}

func copyBytes(w io.Writer, b ByteSequence) (int64, error) {
	s, err := b.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(w, b)
	if err != nil {
		b.Seek(s, io.SeekStart) // best effort repositioning
		return n, err
	}

	_, err = b.Seek(s, io.SeekStart)
	return n, err
}

type bufferedReader struct {
	*bufio.Reader
	limit io.LimitedReader
}

func (b *bufferedReader) reset(r io.Reader, n int64) {
	switch {
	case r == nil:
		b.limit.R = nil
		b.limit.N = 0
		b.Reader.Reset(nil)
	case n < 0:
		b.limit.R = nil
		b.limit.N = 0
		b.Reader.Reset(r)
	default:
		b.limit.R = r
		b.limit.N = n
		b.Reader.Reset(&b.limit)
	}
}

var bufferedReaderPool = sync.Pool{
	New: func() interface{} { return newBufferedReader() },
}

func newBufferedReader() *bufferedReader {
	return &bufferedReader{Reader: bufio.NewReaderSize(nil, 1024)}
}

func acquireBufferedReader(r io.Reader, n int64) *bufferedReader {
	b := bufferedReaderPool.Get().(*bufferedReader)
	b.reset(r, n)
	return b
}

func releaseBufferedReader(b *bufferedReader) {
	b.reset(nil, 0)
	bufferedReaderPool.Put(b)
}
