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

	nextErrs  chan error
	lastChunk pile.ChunkWriter
}

// newWriter creates new writer end of ChunkBuffer
func newWriter(name string, pile pile.Pile, chunkSize int) *writer {
	w := writer{
		name:      name,
		pile:      pile,
		chunkSize: int64(chunkSize),
		buf:       *sharedbuffer.New(),
		nextErrs:  make(chan error),
		lastChunk: nil,
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
	old_written := w.written() - int64(n)
	next, last := w.nextChunk(old_written), w.written()/cs

	errors := w.nextErrs
	w.nextErrs = make(chan error)

	// wait on this write's last chunk if it's complete
	if w.written()%cs == 0 {
		last += 1
	}

	// initialize source readers for complete chunks
	srcsN := 0
	if last-next > 0 {
		srcsN = int(last - next)
	}
	srcs := make([]io.ReadCloser, srcsN)
	for c := next; c < last; c += 1 {
		srcs[c-next], err = w.sourceChunk(c)
		if err != nil {
			return
		}
	}

	// start writing chunks from source readers
	for c := next; c < last; c += 1 {
		go w.writeChunk(srcs[c-next], c, errors)
	}

	// consider waiting on previously incomplete chunk
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
		var src io.ReadCloser
		src, err = w.sourceChunk(last)
		if err == nil {
			go w.writeChunk(src, last, w.nextErrs)
		}
	}

	return
}

// Close the writer, flushing the rest of the buffer to a partial chunk
func (w *writer) Close() error {
	w.buf.Close()

	var err error
	if w.written()%w.chunkSize != 0 {
		err = <-w.nextErrs
	}
	w.nextErrs = nil

	if err == nil && w.lastChunk != nil {
		err = w.lastChunk.SetLast()
	}

	return err
}

// writeChunk of fixed size (specified in ChunkBuffer) to the pile
func (w *writer) writeChunk(src io.ReadCloser, n int64, err chan<- error) {
	ch, writeErr := w.pile.ChunkWriter(w.name, int(n))
	if writeErr != nil {
		err <- writeErr
		return
	}
	w.lastChunk = ch

	_, writeErr = io.Copy(ch, io.LimitReader(src, w.chunkSize))
	src.Close()

	if closeErr := ch.Close(); writeErr == nil {
		writeErr = closeErr
	}
	err <- writeErr
}

// sourceChunk produces a partial reader for the source buffer
func (w *writer) sourceChunk(n int64) (io.ReadCloser, error) {
	if ra, err := w.buf.NewReaderAt(w.chunkSize * n); err != nil {
		return nil, err
	} else {
		return ra, nil
	}
}

// nextChunk number
func (w writer) nextChunk(bytes int64) int64 {
	return (bytes + w.chunkSize - 1) / w.chunkSize
}
