package chunkbuffer

import (
	"errors"
	"github.com/bitantics/chunkbuffer/pile"
	"github.com/bitantics/moreio/meters"
	"github.com/bitantics/moreio/rollingreader"
	"io"
)

var ErrLastChunk = errors.New("last chunk")

// reader end of ChunkBuffer
type reader struct {
	name    string
	pile    pile.Pile
	workers int
	rr      rollingreader.RollingReader
	started bool
}

// newReader creates new reader end of ChunkBuffer
func newReader(name string, pile pile.Pile, workers int) *reader {
	return &reader{
		name:    name,
		pile:    pile,
		workers: workers,
		rr:      *rollingreader.New(),
		started: false,
	}
}

// Read available data from the ChunkBuffer. Block if no data is ready.
func (r *reader) Read(p []byte) (n int, err error) {
	if !r.started {
		r.started = true
		for w := 0; w < r.workers; w += 1 {
			r.startReadingChunk(w)
		}
	}
	if n, err = r.rr.Read(p); err == ErrLastChunk {
		err = io.EOF
		r.rr.Close()
	}
	return
}

// startReadingChunk, but don't wait for any data
func (r *reader) startReadingChunk(n int) {
	ch, err := r.pile.ChunkReader(r.name, n)
	if err != nil {
		r.rr.AddError(err)
		return
	}

	// Wrap chunk in a meter, so we can detect EOF later
	m := meters.NewReadMeter(&readerChunk{ch})
	r.rr.Add(m)

	// If this is not the last chunk, then start downloading the next
	// pending chunk. Last() may block, but this function shouldn't block.
	// So it's in a goroutine.
	go func() {
		<-m.WaitForEOF()
		if !ch.Last() {
			r.startReadingChunk(n + r.workers)
		}
	}()
}

// readerChunk is a wrapper for ChunkReaders. It returns ErrLastChunk
// instead of io.EOF on the last chunk. The error bubbles up through
// the RollingReader, alerting the ChunkBuffer reader it should return
// io.EOF.
type readerChunk struct {
	ch pile.ChunkReader
}

// Read data from the underlying chunk, transforming any io.EOF errors
// to ErrLastChunk if it's the last chunk.
func (rc *readerChunk) Read(p []byte) (n int, err error) {
	n, err = rc.ch.Read(p)
	if err == io.EOF && rc.ch.Last() {
		err = ErrLastChunk
	}
	return
}
