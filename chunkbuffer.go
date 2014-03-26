package chunkbuffer

import (
	"github.com/bitantics/chunkbuffer/pile"
	"errors"
)

type ChunkBuffer struct {
	Name string
	pile *pile.Pile
}

func New(name string, pile *pile.Pile) *ChunkBuffer {
	return &ChunkBuffer{
		Name: name,
		pile: pile,
	}
}

func (cb *ChunkBuffer) Write(p []byte) (n int, err error) {
	return 0, errors.New("Not Implemented")
}

func (cb *ChunkBuffer) Read(p []byte) (n int, err error) {
	return 0, errors.New("Not Implemented")
}

func (cb *ChunkBuffer) Close() error {
	return errors.New("Not Implemented")
}
