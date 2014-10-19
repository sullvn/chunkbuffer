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

	return file
}
