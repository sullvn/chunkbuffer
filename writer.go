package chunkbuffer

import (
	"github.com/bitantics/chunkbuffer/pile"
	"github.com/bitantics/moreio/meters"
	"github.com/bitantics/moreio/sharedbuffer"
	"io"
)

// writer end of ChunkBuffer
type writer struct {
	name      string
	pile      pile.Pile
	chunkSize int64

	buf sharedbuffer.SharedBuffer
	wm  meters.WriteMeter

	nextErrs chan error
}

// newWriter creates new writer end of ChunkBuffer
func newWriter(name string, pile pile.Pile, chunkSize int) *writer {
	w := writer{
		name:      name,
		pile:      pile,
		chunkSize: int64(chunkSize),
		buf:       *sharedbuffer.New(),
		nextErrs:  make(chan error),
	}
	w.wm = *meters.NewWriteMeter(&w.buf)
	return &w
}

// written byte count so far
func (w *writer) written() int64 {
	return w.wm.Reading()
}

// Write data into the ChunkBuffer, blocking until complete.
// Will block for all chunks which are completed as part of this write. This
// means it will block for the previously incomplete, last chunk, but not for
// the newly incomplete, last chunk.
func (w *writer) Write(p []byte) (n int, err error) {
	n, err = w.wm.Write(p)
	if err != nil {
		return
	}

	cs := w.chunkSize
	next, last := w.nextChunk(w.written()), w.written()/cs
	old_written := w.written() - int64(n)

	errors := w.nextErrs
	w.nextErrs = make(chan error)

	// wait on this write's last chunk if it's complete
	if w.written()%cs == 0 {
		last += 1
	}

	// start complete chunks
	for c := next; c < last; c += 1 {
		go w.writeChunk(c, errors)
	}

	if old_written%cs != 0 {
		next -= 1
	}

	// wait for complete chunk writes and check for errors
	for c := next; c < last; c += 1 {
		if e := <-errors; e != nil && err == nil {
			err = e
		}
	}
	if err != nil {
		return
	}

	// start next incomplete chunk
	if w.nextChunk(w.written()) > w.nextChunk(old_written) {
		go w.writeChunk(last, w.nextErrs)
	}

	return
}

// Close the writer, flushing the rest of the buffer to a partial chunk
func (w *writer) Close() error {
	go w.buf.Close()

	var err error
	if w.written()%w.chunkSize != 0 {
		err = <-w.nextErrs
	}
	w.nextErrs = nil

	if err == nil {
		err = w.pile.LastChunk(w.name, int(w.nextChunk(w.written())-1))
	}

	return err
}

// writeChunk of fixed size (specified in ChunkBuffer) to the pile
func (w *writer) writeChunk(n int64, err chan<- error) {
	cs := w.chunkSize

	ra := w.buf.NewReaderAt(cs * n)
	ch := w.pile.Chunk(w.name, int(n))

	_, writeErr := io.Copy(ch, io.LimitReader(ra, cs))
	ra.Close()

	if closeErr := ch.Close(); err == nil {
		writeErr = closeErr
	}
	err <- writeErr
}

// nextChunk number
func (w writer) nextChunk(bytes int64) int64 {
	return (bytes + w.chunkSize - 1) / w.chunkSize
}
