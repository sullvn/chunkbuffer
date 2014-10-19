/*
chunkbuffer provides a buffer on top of an arbitrary object store.

To use a ChunkBuffer, you simply make and configure a Pile for the
storage backend you want to use. Then you call `chunkbuffer.New`
with your buffer's unique name and the Pile. This enables you
to buffer anything using memory, the disk, the cloud, or anything
other method.

Happy limitless buffering!
*/
package chunkbuffer

import (
	"errors"
	"github.com/bitantics/chunkbuffer/pile"
)

// ChunkBuffer is a buffer working on a storage backend
type ChunkBuffer struct {
	Name string
	pile *pile.Pile
}

// New creates a ChunkBuffer
func New(name string, pile *pile.Pile) *ChunkBuffer {
	return &ChunkBuffer{
		Name: name,
		pile: pile,
	}
}

// Write pushes data into the buffer
func (cb *ChunkBuffer) Write(p []byte) (n int, err error) {
	return 0, errors.New("Not Implemented")
}

// Read pulls data from the buffer
func (cb *ChunkBuffer) Read(p []byte) (n int, err error) {
	return 0, errors.New("Not Implemented")
}

// Close finalizes any in-progress reads and writes
func (cb *ChunkBuffer) Close() error {
	return errors.New("Not Implemented")
}
