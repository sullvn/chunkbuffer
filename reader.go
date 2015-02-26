package chunkbuffer

import (
	"github.com/bitantics/chunkbuffer/pile"
	"github.com/bitantics/moreio/meters"
	"github.com/bitantics/moreio/rollingreader"
)

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
	return r.rr.Read(p)
}

// startReadingChunk, but don't wait for any data
func (r *reader) startReadingChunk(n int) {
	ch := r.pile.Chunk(r.name, n)
	m := meters.NewReadMeter(ch)
	r.rr.Add(m)

	// If this is the last chunk, then signify to the ChunkBuffer by
	// closing the RollingReader.
	// Last() may block, but this function shouldn't block. So it's in
	// a goroutine.
	go func() {
		<-m.WaitForEOF()
		if ch.Last() {
			r.rr.Close()
		} else {
			r.startReadingChunk(n + r.workers)
		}
	}()
}
