package records

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrClosed is returned when using a storage instance that has already
	// been closed.
	ErrClosed = errors.New("closed")
	// ErrNotFound is returned when an attempt to load a record batch from
	// a cache did not find the requested offset.
	ErrNotFound = errors.New("not found")
)

// The Iter interface abstracts the underlying implementation of iterators
// returned by calling List on a storage instance.
//
// Iterators must be closed when the application does not need them anymore in
// order to release the resources it held.
type Iter interface {
	io.Closer

	// Next must be called to position the iterator on the next storage entry.
	//
	// When newly created, iterators are positioned right before the first entry
	// in the storage instance they were created from.
	Next() bool

	// Returns the key that the iterator is currently positioned on.
	Key() Key

	// Returns the size of the value that the iterator is currently positioned
	// on.
	Size() int64
}

// The Storage interface is used to abstract the location where a cache writes
// records.
//
// The package provides two implementations of this interface, one for in-memory
// storage and the other to record entries on the file system.
//
// Storage instances must be safe to use concurrently from multiple goroutines.
type Storage interface {
	// Lists the keys of all record batches currently present in the store.
	List(ctx context.Context) Iter

	// Loads the value matching the given key in the store.
	//
	// Unless an error occurs, in which case the returned error will not be nil,
	// the caller is responsible for closing the value reader it received when
	// it does not need it anymore to release resources.
	//
	// If the key could not be found, ErrNotFound is returned.
	Load(ctx context.Context, key Key) (io.ReadCloser, error)

	// Stores a key/value pair.
	//
	// The method returns the size of the value written to the store, and any
	// error that occured.
	Store(ctx context.Context, key Key, value io.WriterTo) (int64, error)

	// Deletes entries matching the given keys from the store.
	//
	// The operation is idempotent, it returns no errors if one or more keys did
	// not exist in the store.
	Delete(ctx context.Context, keys []Key) error
}

// NewStorage constructs a new storage instance which records values in memory.
func NewStorage() Storage { return new(storage) }

type storage struct {
	mutex sync.RWMutex
	pool  sync.Pool
	state map[Key]*bytes.Buffer
}

func (s *storage) List(ctx context.Context) Iter {
	return &storageIter{entries: s.entries(), index: -1}
}

func (s *storage) entries() (entries []storageEntry) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entries = make([]storageEntry, 0, len(s.state))
	for key, value := range s.state {
		entries = append(entries, storageEntry{
			key:  key,
			size: int64(value.Len()),
		})
	}
	return entries
}

func (s *storage) Load(ctx context.Context, key Key) (io.ReadCloser, error) {
	s.mutex.RLock()
	value, found := s.state[key]
	s.mutex.RUnlock()
	if !found {
		return nil, ErrNotFound
	}
	reader := new(storageValue)
	reader.Reset(value.Bytes())
	return reader, nil
}

func (s *storage) Store(ctx context.Context, key Key, value io.WriterTo) (int64, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(256 * 1024)

	n, err := value.WriteTo(buffer)
	if err != nil {
		return 0, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.state == nil {
		s.state = make(map[Key]*bytes.Buffer)
	}

	s.state[key] = buffer
	return n, nil
}

func (s *storage) Delete(ctx context.Context, keys []Key) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, key := range keys {
		delete(s.state, key)
	}

	return nil
}

type storageEntry struct {
	key  Key
	size int64
}

type storageValue struct {
	bytes.Reader
}

func (r *storageValue) Close() error {
	r.Reset(nil)
	return nil
}

type storageIter struct {
	entries []storageEntry
	index   int
}

func (it *storageIter) Close() error {
	it.index = len(it.entries)
	return nil
}

func (it *storageIter) Next() bool {
	it.index++
	return it.index < len(it.entries)
}

func (it *storageIter) Key() Key {
	if it.index >= 0 && it.index < len(it.entries) {
		return it.entries[it.index].key
	}
	return Key{}
}

func (it *storageIter) Size() int64 {
	if it.index >= 0 && it.index < len(it.entries) {
		return it.entries[it.index].size
	}
	return 0
}

// MountPoint is an implementation of the Storage interface which keeps track of
// records in files on the local file system.
//
// The storage strategy implemented by this type uses the following directory
// structure:
//
//	address
//	└── topic
//	    └── partition
//	        └── bucket
//	            └── records
//
// Directories at the partition level are named after the partition number that
// they are storing records for (e.g. 0, 1, 2, etc...).
//
// There are up to 256 bucket directories per partition, each numbered with a
// 2-digit hexadecimal value. The purpose of those buckets is to avoid keeping
// too many record files in a single directory, amortizing the cost of mutations
// on the directories.
//
// The record files are named by the decimal representation of the base offset
// they start at, and contain the raw values, unmodified.
//
// New entries are recorded in temporary files using the following naming
// scheme:
//
//		{baseOffset}.{numRecords}.{uid}~
//
// The use of the "~" suffix allows the MountPoint implementation to detect and
// skip temporary files when scanning the directory contents. The use of unique
// identifier for each temporary file prevents conflicts between concurrent
// writes to the same key, reconciliation happens atomically when the temporary
// files are renamed to their target name.
//
// This directory structure is optimized for simplicity of operations, it is a
// useful default for a lot of applications but may not suit programs that would
// exercise edge case behaviors (e.g. having a lot of very small record batches
// would result in a large space overhead occupied by file entries); those
// applications would be encouraged to avoid using this implementation and
// instead invest in providing a custom implementation of the Storage interface
// which better fits their needs.
type MountPoint struct {
	// mutable mount point state, synchronized on mutex
	mutex  sync.RWMutex
	closed bool
	files  fileCache
	// immutable mount point configuration
	path          string
	fileCacheSize int
}

// MountOption is an interface used to apply configuration options when creating
// a mount point.
type MountOption interface {
	ConfigureMountPoint(*MountPoint)
}

type mountOption func(*MountPoint)

func (opt mountOption) ConfigureMountPoint(mp *MountPoint) { opt(mp) }

// FileCacheSize is a mount point configuration option setting the limit of
// cached open files.
//
// Defaults to zeor, which means no caching is applied.
func FileCacheSize(numFiles int) MountOption {
	return mountOption(func(mp *MountPoint) { mp.fileCacheSize = numFiles })
}

// Mount creates a MountPoint instance from the given file system path.
//
// The path must exist, otherwise the function will error.
//
// The returned mount point must be closed to release system resources when the
// application does not need it anymore.
func Mount(path string, options ...MountOption) (*MountPoint, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("mounting kafka records storage: %w", err)
	}
	mp := &MountPoint{path: path}
	for _, opt := range options {
		opt.ConfigureMountPoint(mp)
	}
	return mp, nil
}

// Close release the resources held by the mount point. It must be closed when
// the application does not need the mount point anymore.
func (mp *MountPoint) Close() error {
	mp.mutex.Lock()
	mp.closed = true
	mp.mutex.Unlock()
	mp.files.reset()
	return nil
}

// Path returns the path that the mount point was mounted at.
func (mp *MountPoint) Path() string {
	return mp.path
}

// List satisfies the Storage interface.
func (mp *MountPoint) List(ctx context.Context) Iter {
	return &mountPointIter{
		mountPoint: mp,
		addrs:      readdir(mp.path),
	}
}

// Load satisfies the Storage interface.
func (mp *MountPoint) Load(ctx context.Context, key Key) (io.ReadCloser, error) {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	if mp.closed {
		return nil, ErrClosed
	}

	ref := mp.files.lookup(key)
	if ref == nil {
		f, err := os.Open(mp.pathTo(key))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				err = ErrNotFound
			}
			return nil, err
		}
		if mp.fileCacheSize > 0 {
			ref = mp.files.insert(key, f, mp.fileCacheSize)
		} else {
			return f, nil
		}
	}
	return &fileReadCloser{ref: ref}, nil
}

// Store satisfies the Storage interface.
func (mp *MountPoint) Store(ctx context.Context, key Key, value io.WriterTo) (int64, error) {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	if mp.closed {
		return 0, ErrClosed
	}

	dst := mp.pathTo(key)

	f, err := mp.createTemp(dst)
	if err != nil {
		return 0, err
	}
	tmp := f.Name()
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()

	n, err := value.WriteTo(f)
	if err != nil {
		return n, err
	}
	if err := f.Sync(); err != nil {
		return n, err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return n, err
	}

	if mp.fileCacheSize > 0 {
		mp.files.insert(key, f, mp.fileCacheSize).unref()
		f = nil
	}
	return n, nil
}

// Delete satisfies the Storage interface.
func (mp *MountPoint) Delete(ctx context.Context, keys []Key) error {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	if mp.closed {
		return ErrClosed
	}

	dst := new(strings.Builder)

	for _, key := range keys {
		dst.Reset()
		mp.writePathTo(dst, key)
		mp.files.delete(key)

		if err := os.Remove(dst.String()); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}

	return nil
}

func (mp *MountPoint) createTemp(filePath string) (*os.File, error) {
	dir, base := filepath.Dir(filePath), filepath.Base(filePath)+".*~"

	f, err := os.CreateTemp(dir, base)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := mp.makePath(dir); err != nil {
				return nil, err
			}
			return os.CreateTemp(dir, base)
		}
	}

	return f, err
}

func (mp *MountPoint) makePath(subpath string) error {
	if mp.path == subpath {
		return nil // root
	}
	if err := mp.makePath(filepath.Dir(subpath)); err != nil {
		return err
	}
	err := os.Mkdir(subpath, 0755)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			err = nil
		}
	}
	return err
}

func (mp *MountPoint) parseKey(s string) (key Key, err error) {
	basePath := s

	errorf := func(msg string, args ...interface{}) error {
		return &os.PathError{
			Op:   "open",
			Path: basePath,
			Err:  fmt.Errorf(msg, args...),
		}
	}

	if !strings.HasPrefix(basePath, mp.path) {
		return Key{}, errorf("not in path: %s", mp.path)
	}

	sep := string([]byte{os.PathSeparator})
	s = strings.TrimPrefix(s, mp.path)
	s = strings.TrimPrefix(s, sep)

	key.Addr, s = splitNextPathPart(s)
	key.Topic, s = splitNextPathPart(s)
	partition, s := splitNextPathPart(s)
	_, s = splitNextPathPart(s) // bucket
	fileName, _ := splitNextPathPart(s)

	partitionID, err := strconv.ParseInt(partition, 10, 32)
	if err != nil {
		return key, errorf("malformed partition number: %w", err)
	}

	fileNameBaseOffset, fileNameLastOffsetDelta := split(fileName, '.')
	lastOffsetDelta, err := strconv.ParseInt(fileNameLastOffsetDelta, 10, 32)
	if err != nil {
		return key, errorf("malformed number of records: %w", err)
	}

	baseOffset, err := strconv.ParseInt(fileNameBaseOffset, 10, 64)
	if err != nil {
		return key, errorf("malformed base offset: %w", err)
	}

	key.Partition = int32(partitionID)
	key.LastOffsetDelta = int32(lastOffsetDelta)
	key.BaseOffset = baseOffset
	return key, nil
}

func splitNextPathPart(s string) (part, next string) {
	return split(s, os.PathSeparator)
}

func split(s string, c byte) (prefix, suffix string) {
	if i := strings.IndexByte(s, c); i < 0 {
		return s, ""
	} else {
		return s[:i], s[i+1:]
	}
}

func (mp *MountPoint) pathTo(k Key) string {
	s := new(strings.Builder)
	mp.writePathTo(s, k)
	return s.String()
}

func (mp *MountPoint) writePathTo(s *strings.Builder, k Key) {
	writeDirString(s, mp.path)
	writeStorageKey(s, k)
}

func writeDirString(s *strings.Builder, p string) {
	s.WriteString(p)
	s.WriteByte(os.PathSeparator)
}

func writeDirBytes(s *strings.Builder, p []byte) {
	s.Write(p)
	s.WriteByte(os.PathSeparator)
}

func writeStorageKey(s *strings.Builder, k Key) {
	b := make([]byte, 0, 128)
	b = append(b, k.Addr...)
	b = append(b, k.Topic...)
	b = appendUint64(b, uint64(k.Partition))
	b = appendUint64(b, uint64(k.BaseOffset))
	hash := sha256.Sum256(b)
	bucket := hexByte(hash[0])

	writeDirString(s, filepath.Clean(k.Addr))
	writeDirString(s, filepath.Clean(k.Topic))
	writeDirBytes(s, strconv.AppendUint(b[:0], uint64(k.Partition), 10))
	writeDirBytes(s, bucket[:])

	b = strconv.AppendUint(b[:0], uint64(k.BaseOffset), 10)
	n := 20 - len(b)
	b = b[:20]
	copy(b[n:], b)

	for i := 0; i < n; i++ {
		b[i] = '0'
	}

	s.Write(b)
	s.WriteByte('.')
	s.Write(strconv.AppendUint(b[:0], uint64(k.LastOffsetDelta), 10))
}

func appendUint64(b []byte, u uint64) []byte {
	i := len(b)
	b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(b[i:], u)
	return b
}

func hexByte(b byte) (hex [2]byte) {
	const hexchars = "0123456789ABCDEF"
	hex[0] = hexchars[b>>4]
	hex[1] = hexchars[b&0xF]
	return
}

type mountPointIter struct {
	mountPoint *MountPoint
	addrs      dir
	topics     dir
	partitions dir
	buckets    dir
	offsets    dir
	err        error
	key        Key
	size       int64
}

func (it *mountPointIter) Close() error {
	it.key, it.size = Key{}, 0

	for _, err := range []error{
		it.err,
		it.offsets.close(),
		it.buckets.close(),
		it.partitions.close(),
		it.topics.close(),
		it.addrs.close(),
	} {
		if err != nil {
			return err
		}
	}

	return nil
}

func (it *mountPointIter) Next() bool {
	if it.err != nil {
		goto done
	}

nextOffset:
	if it.offsets.next() {
		fileName := it.offsets.name()
		if strings.HasSuffix(fileName, "~") {
			goto nextOffset // skip temporary files
		}

		k, err := it.mountPoint.parseKey(fileName)
		if err != nil {
			goto nextBucket
		}

		s, err := os.Lstat(fileName)
		if err != nil {
			it.err = err
			goto done
		}

		it.key, it.size = k, s.Size()
		return true
	}

	if it.err = it.offsets.close(); it.err != nil {
		goto done
	}

nextBucket:
	if it.buckets.next() {
		it.offsets = it.buckets.read()
		goto nextOffset
	}

	if it.err = it.buckets.close(); it.err != nil {
		goto done
	}

nextPartition:
	if it.partitions.next() {
		it.buckets = it.partitions.read()
		goto nextBucket
	}

	if it.err = it.partitions.close(); it.err != nil {
		goto done
	}

nextTopic:
	if it.topics.next() {
		it.partitions = it.topics.read()
		goto nextPartition
	}

	if it.err = it.topics.close(); it.err != nil {
		goto done
	}

	if it.addrs.next() {
		it.topics = it.addrs.read()
		goto nextTopic
	}

	it.err = it.addrs.close()
done:
	return false
}

func (it *mountPointIter) Key() Key {
	return it.key
}

func (it *mountPointIter) Size() int64 {
	return it.size
}

type dir struct {
	file  *os.File
	path  string
	names []string
	index int
	err   error
}

func (d *dir) opened() bool {
	return d.file != nil
}

func (d *dir) close() error {
	if d.file != nil {
		d.file.Close()
		d.file = nil
	}
	d.file = nil
	d.names = nil
	d.index = 0
	return d.err
}

func (d *dir) next() bool {
	if d.file == nil {
		return false
	}

	if d.index++; d.index < len(d.names) {
		return true
	}

	names, err := d.file.Readdirnames(100)
	switch err {
	case nil:
		d.names, d.index = names, 0
		return true
	case io.EOF:
		d.names, d.index = nil, 0
		return false
	default:
		d.err = err
		d.close()
		return false
	}
}

func (d *dir) name() string {
	if d.index >= 0 && d.index < len(d.names) {
		return filepath.Join(d.path, d.names[d.index])
	}
	return ""
}

func (d *dir) read() dir {
	return readdir(d.name())
}

func readdir(path string) dir {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	}
	return dir{path: path, file: f, err: err}
}

type fileReadCloser struct {
	ref *fileRef
	off int64
}

func (r *fileReadCloser) Close() error {
	if r.ref != nil {
		r.ref.unref()
		r.ref = nil
	}
	return nil
}

func (r *fileReadCloser) Read(b []byte) (int, error) {
	if r.ref != nil {
		n, err := r.ref.file.ReadAt(b, r.off)
		r.off += int64(n)
		return n, err
	}
	return 0, io.EOF
}

type fileRef struct {
	refc uintptr
	key  Key
	file *os.File
	elem *list.Element
}

func (f *fileRef) ref() {
	atomic.AddUintptr(&f.refc, 1)
}

func (f *fileRef) unref() {
	if atomic.AddUintptr(&f.refc, ^uintptr(0)) == 0 {
		f.file.Close()
		f.file = nil
	}
}

type fileCache struct {
	mutex sync.Mutex
	queue list.List
	files map[Key]*fileRef
}

func (c *fileCache) reset() {
	c.mutex.Lock()
	files := c.files
	c.files = nil
	c.queue = list.List{}
	c.mutex.Unlock()

	for _, file := range files {
		file.unref()
	}
}

func (c *fileCache) insert(key Key, file *os.File, limit int) *fileRef {
	newRef := &fileRef{refc: 2, key: key, file: file}
	oldRef := (*fileRef)(nil)
	evictRef := (*fileRef)(nil)

	defer func() {
		if oldRef != nil {
			oldRef.unref()
		}
		if evictRef != nil {
			evictRef.unref()
		}
	}()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.files == nil {
		c.files = make(map[Key]*fileRef)
	} else {
		if oldRef = c.files[key]; oldRef != nil {
			c.queue.Remove(oldRef.elem)
		}
	}

	c.files[key] = newRef
	newRef.elem = c.queue.PushFront(newRef)

	if c.queue.Len() > limit {
		evictRef = c.queue.Back().Value.(*fileRef)
		c.queue.Remove(evictRef.elem)
		delete(c.files, evictRef.key)
	}

	return newRef
}

func (c *fileCache) lookup(key Key) *fileRef {
	c.mutex.Lock()
	file := c.files[key]
	if file != nil {
		file.ref()
	}
	c.mutex.Unlock()
	return file
}

func (c *fileCache) delete(key Key) {
	var file *fileRef

	defer func() {
		if file != nil {
			file.unref()
		}
	}()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if file = c.files[key]; file != nil {
		c.queue.Remove(file.elem)
	}
}
