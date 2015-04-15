/*
pile provides abstract chunk storage methods for a ChunkBuffer.

In other words, you can store chunks of a binary file using an
arbitrary method. This may be in memory, on the filesystem, over
the network, or any other way. As long as the client uses the
the Pile interface, it can swap out the methods without worry.
*/
package pile

import (
	"errors"
	"io"
)

var ErrNotFound = errors.New("pile could not locate the chunk")

// ChunkReader enables reading a chunk and checking if it's
// the last part
type ChunkReader interface {
	io.ReadCloser
	Last() bool
}

// ChunkWriter enables writing a chunk and optionally marking
// it as the last part
type ChunkWriter interface {
	io.WriteCloser
	SetLast() error
}

// Pile abstracts an underlying method of storing chunks
type Pile interface {
	ChunkReader(name string, part int) (ChunkReader, error)
	ChunkWriter(name string, part int) (ChunkWriter, error)
}
