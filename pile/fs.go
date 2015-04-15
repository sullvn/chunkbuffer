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

// pathTo chunk file
func (fp FilePile) pathTo(name string, part int) string {
	return filepath.Join(fp.dir, name, strconv.Itoa(part))
}

func (fp FilePile) ChunkWriter(name string, part int) (ChunkWriter, error) {
	filename := fp.pathTo(name, part)
	dir := filepath.Dir(filename)

	// make sure the directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
	}

	// reuse existing file if possible
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if os.IsNotExist(err) {
		file, err = os.Create(filename)
	}

	return fileChunk{file, fp, name, part}, err
}

// ChunkReader returns a file based chunk reader
func (fp FilePile) ChunkReader(name string, part int) (ChunkReader, error) {
	filename := fp.pathTo(name, part)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = ErrNotFound
	}

	return fileChunk{file, fp, name, part}, err
}

// fileChunk implements the Chunk interface for files
type fileChunk struct {
	*os.File
	fp   FilePile
	name string
	part int
}

// Last chunk if the file doesn't exist or is empty
func (fc fileChunk) Last() bool {
	if fi, err := fc.Stat(); err != nil || fi.Size() == 0 {
		return true
	}
	return false
}

// SetLast chunk by creating an empty subsequent chunk file
func (fc fileChunk) SetLast() error {
	ch, err := fc.fp.ChunkWriter(fc.name, fc.part+1)
	if err == nil {
		_, err = ch.Write([]byte{})
	}
	if err == nil {
		ch.Close()
	}
	return err
}
