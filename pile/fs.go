package pile

import (
	"os"
	"path/filepath"
	"strconv"
)

// FilePile is an instance of a file based pile
type FilePile struct {
	dir string
}

// NewTempFilePile creates a file based pile in a temporary directory
func NewTempFilePile() FilePile {
	return FilePile{dir: os.TempDir()}
}

// Chunk returns a file based chunk
func (fp FilePile) Chunk(name string, part int) Chunk {
	filename := filepath.Join(fp.dir, name, strconv.Itoa(part))
	dir := filepath.Dir(filename)

	// make sure the directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
	}

	// reuse existing file if possible
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		file, err = os.Create(filename)
	}

	return fileChunk{file}
}

// LastChunk is marked by an subsequent next chunk
func (fp FilePile) LastChunk(name string, part int) error {
	_, err := fp.Chunk(name, part+1).Write([]byte{})
	return err
}

// fileChunk implements the Chunk interface for files
type fileChunk struct {
	*os.File
}

// Last chunk if the file doesn't exist or is non-empty
func (fc fileChunk) Last() bool {
	if fi, err := fc.Stat(); err != nil || fi.Size() == 0 {
		return true
	}
	return false
}
