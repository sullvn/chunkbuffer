package chunkbuffer

import (
	"bytes"
	"crypto/rand"
	"github.com/bitantics/chunkbuffer/pile"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"testing"
)

func TestChunkBuffer(t *testing.T) {
	data := make([]byte, (CHUNK_SIZE*5)+CHUNK_SIZE/3)
	rand.Read(data)
	in := bytes.NewReader(data)

	fp := pile.NewTempFilePile()

	Convey("Given a new chunkbuffer using the filesystem", t, func() {
		cb := New("test", fp)

		Convey("When some data is written to it", func() {
			written, err := io.Copy(cb, in)
			So(written, ShouldEqual, len(data))
			So(err, ShouldBeNil)

			cb.Close()

			Convey("Then it should be read out, fully intact", func() {
				out, err := ioutil.ReadAll(cb)
				So(err, ShouldBeNil)
				So(out, ShouldResemble, data)
			})
		})

		Convey("When data is written in small pieces", func() {
			small, d := CHUNK_SIZE/5, make([]byte, 0)
			copy(d, data)

			for len(d) > small {
				cb.Write(d[:small])
				d = d[small:]
			}
			if len(d) > 0 {
				cb.Write(d)
			}

			Convey("Then it should be read out, fully intact", func() {
				out, err := ioutil.ReadAll(cb)
				So(err, ShouldBeNil)
				So(out, ShouldResemble, data)
			})
		})
	})
}
