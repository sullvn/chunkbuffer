/*
pile provides abstract chunk storage methods for a ChunkBuffer.

In other words, you can store chunks of a binary file using an
arbitrary method. This may be in memory, on the filesystem, over
the network, or any other way. As long as the client uses the
the Pile interface, it can swap out the methods without worry.
*/
package pile

import (
	"io"
)

// Chunk is a piece of a binary object
type Chunk interface {
	io.ReadWriteCloser
	Last() bool
}

// Pile abstracts an underlying method of storing chunks
type Pile interface {
	Chunk(name string, part int) Chunk
	LastChunk(name string, part int) error
}
