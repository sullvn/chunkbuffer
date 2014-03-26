package pile

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"

	"os"
	"path/filepath"
	"io"
	"io/ioutil"
)

var TEST_NAME = "chunkbuffer-fp-test"

func TestFilePile(t *testing.T) {
	dir := os.TempDir()
	filename := filepath.Join(dir, TEST_NAME, "0")

	fp := FilePile{dir: dir}
	chunk := fp.Chunk(TEST_NAME, 0)
	io.WriteString(chunk, "test")
	chunk.Close()

	Convey("Created correct files", t, func() {
		_, err := os.Stat(filename)

		So(os.IsNotExist(err), ShouldBeFalse)
	})

	Convey("Read chunk data", t, func() {
		chunk := fp.Chunk(TEST_NAME, 0)
		data, _ := ioutil.ReadAll(chunk)

		So(string(data), ShouldEqual, "test")
	})

	os.RemoveAll(filepath.Join(dir, TEST_NAME))
}
