package records

import (
	"bytes"
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

	"github.com/segmentio/kafka-go/records/cache"
)

var (
	// ErrNotFound is returned when an attempt to load a record batch from
	// a cache did not find the requested offset.
	ErrNotFound = errors.New("not found")
)

// Key is a comparable type representing the identifiers that record batches
// can be indexed by in a storage instance.
type Key = cache.Key

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
	state map[Key][]byte
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
			size: int64(len(value)),
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
	reader.Reset(value)
	return reader, nil
}

func (s *storage) Store(ctx context.Context, key Key, value io.WriterTo) (int64, error) {
	buffer := new(bytes.Buffer)
	n, err := value.WriteTo(buffer)
	if err != nil {
		return 0, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.state == nil {
		s.state = make(map[Key][]byte)
	}

	s.state[key] = buffer.Bytes()
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
type MountPoint string

func Mount(path string) (MountPoint, error) {
	root, err := filepath.Abs(path)
	return MountPoint(root), err
}

func (path MountPoint) List(ctx context.Context) Iter {
	return &mountPointIter{
		mountPoint: path,
		addrs:      readdir(string(path)),
	}
}

func (path MountPoint) Load(ctx context.Context, key Key) (io.ReadCloser, error) {
	f, err := os.Open(path.pathTo(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = ErrNotFound
		}
		return nil, err
	}
	return f, nil
}

func (path MountPoint) Store(ctx context.Context, key Key, value io.WriterTo) (int64, error) {
	dst := path.pathTo(key)

	f, err := path.createTemp(dst)
	if err != nil {
		return 0, err
	}
	tmp := f.Name()
	defer f.Close()
	defer os.Remove(tmp)

	n, err := value.WriteTo(f)
	if err != nil {
		return n, err
	}
	if err := f.Sync(); err != nil {
		return n, err
	}
	if err := f.Close(); err != nil {
		return n, err
	}

	return n, os.Rename(tmp, dst)
}

func (path MountPoint) Delete(ctx context.Context, keys []Key) error {
	dst := new(strings.Builder)

	for _, key := range keys {
		dst.Reset()
		path.writePathTo(dst, key)

		if err := os.Remove(dst.String()); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}

	return nil
}

func (path MountPoint) createTemp(filePath string) (*os.File, error) {
	dir, base := filepath.Dir(filePath), filepath.Base(filePath)+".*~"

	f, err := os.CreateTemp(dir, base)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := path.makePath(dir); err != nil {
				return nil, err
			}
			return os.CreateTemp(dir, base)
		}
	}

	return f, err
}

func (path MountPoint) makePath(subpath string) error {
	if string(path) == subpath {
		return nil // root
	}
	if err := path.makePath(filepath.Dir(subpath)); err != nil {
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

func (path MountPoint) parseKey(s string) (key Key, err error) {
	basePath := s

	errorf := func(msg string, args ...interface{}) error {
		return &os.PathError{
			Op:   "open",
			Path: basePath,
			Err:  fmt.Errorf(msg, args...),
		}
	}

	if !strings.HasPrefix(basePath, string(path)) {
		return Key{}, errorf("not in path: %s", path)
	}

	sep := string([]byte{os.PathSeparator})
	s = strings.TrimPrefix(s, string(path))
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

func (path MountPoint) pathTo(k Key) string {
	s := new(strings.Builder)
	path.writePathTo(s, k)
	return s.String()
}

func (path MountPoint) writePathTo(s *strings.Builder, k Key) {
	writeDirString(s, string(path))
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
	mountPoint MountPoint
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
