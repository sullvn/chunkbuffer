package pile

import (
	"os"
	"path/filepath"
	"strconv"
)

type FilePile struct {
	dir string
}

func NewTempFilePile() FilePile {
	return FilePile{dir: os.TempDir()}
}

func (fp FilePile) Chunk(name string, part int) Chunk {
	filename := filepath.Join(fp.dir, name, strconv.Itoa(part))
	dir := filepath.Dir(filename)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
	}

	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		file, err = os.Create(filename)
	}

	return file
}
