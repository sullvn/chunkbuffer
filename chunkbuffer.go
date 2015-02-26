/*
chunkbuffer provides a buffer over an arbitrary object store.

To use a ChunkBuffer, you simply make and configure a Pile for the
storage backend you want to use. Then you call `chunkbuffer.New`
with your buffer's unique name and the Pile. This enables you
to buffer anything using memory, the disk, the cloud, or anything
other method.

Happy limitless buffering!
*/
package chunkbuffer

import (
	"github.com/bitantics/chunkbuffer/pile"
)

const PARALLEL_WORKERS = 2
const CHUNK_SIZE = 1024 * 1024 * 5

// ChunkBuffer is a buffer working on a storage backend
type ChunkBuffer struct {
	Name string
	pile pile.Pile

	writer *writer
	reader *reader
}

// New creates a ChunkBuffer
func New(name string, pile pile.Pile) *ChunkBuffer {
	cb := &ChunkBuffer{
		Name: name,
		pile: pile,
	}
	cb.writer = newWriter(name, pile, CHUNK_SIZE)
	cb.reader = newReader(name, pile, PARALLEL_WORKERS)
	return cb
}

// Write pushes data into the buffer
func (cb *ChunkBuffer) Write(p []byte) (n int, err error) {
	return cb.writer.Write(p)
}

// Read pulls data from the buffer
func (cb *ChunkBuffer) Read(p []byte) (n int, err error) {
	return cb.reader.Read(p)
}

// Close finalizes any in-progress reads and writes
func (cb *ChunkBuffer) Close() error {
	err := cb.writer.Close()
	return err
}
