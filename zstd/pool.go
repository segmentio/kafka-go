package zstd

import zstdlib "github.com/klauspost/compress/zstd"

// decoderPool is very basic implementation of a pool of zstdlib.Decoders.
//
// A decoderPool is initially empty. Callers should create new decoders as
// necessary and add them to the pool when no longer necessary (via Put).
// If the pool is at capacity when Put is called, any excess decoders will
// be closed and discarded.
//
// This implementation never downsizes.
type decoderPool struct {
	pool chan *zstdlib.Decoder
}

// newDecoderPool creates a new decoderPool with specified maxSize.
func newDecoderPool(maxSize int) *decoderPool {
	return &decoderPool{
		pool: make(chan *zstdlib.Decoder, maxSize),
	}
}

// Get immediately returns an available pooled decoder or nil, if none are
// available.
//
// If nil is returned, caller should create a new zstdlib.Decoder.
// When the decoder is no longer necessary, it should be added/returned to the
// pool with Put.
func (p *decoderPool) Get() *zstdlib.Decoder {
	select {
	case dec := <-p.pool:
		return dec
	default:
		// No available decoders in pool; caller should create
		// one and return it to the pool via Put when done.
		return nil
	}
}

// Put adds a decoder to the pool, if there is available capacity.
// Any excess decoders will be immediately closed and discarded.
func (p *decoderPool) Put(dec *zstdlib.Decoder) {
	select {
	case p.pool <- dec:
	default:
		// No space left in pool; close and discard excess decoders.
		dec.Close()
	}
}
