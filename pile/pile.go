package pile

import (
	"io"
)

type Chunk interface {
	io.ReadWriteCloser
}

type Pile interface {
	Chunk(name string, part int) Chunk
}
