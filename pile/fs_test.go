package pile

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"

	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

var TEST_NAME = "chunkbuffer-fp-test"

func TestFilePile(t *testing.T) {
	dir := os.TempDir()
	fp := FilePile{dir: dir}

	Convey("Given a written chunk", t, func() {
		chunk, err := fp.ChunkWriter(TEST_NAME, 0)
		So(err, ShouldBeNil)

		io.WriteString(chunk, "test")
		chunk.Close()

		Convey("Then the correct files were made", func() {
			_, err := os.Stat(filepath.Join(dir, TEST_NAME, "0"))

			So(os.IsNotExist(err), ShouldBeFalse)
		})

		Convey("Then the correct data is read", func() {
			chunk, err := fp.ChunkReader(TEST_NAME, 0)
			So(err, ShouldBeNil)

			data, _ := ioutil.ReadAll(chunk)
			So(string(data), ShouldEqual, "test")
		})

		os.RemoveAll(filepath.Join(dir, TEST_NAME))
	})
}
