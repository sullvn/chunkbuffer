package pile

import (
	"crypto/rand"
	"github.com/awslabs/aws-sdk-go/aws"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"os"
	"testing"
)

func TestS3Pile(t *testing.T) {
	sp := NewS3Pile(aws.DefaultConfig, os.Getenv("S3_BUCKET"))
	in := make([]byte, 500*1000)
	rand.Read(in)

	Convey("When writing a chunk to S3", t, func() {
		cw, err := sp.ChunkWriter("test", 0)
		So(err, ShouldBeNil)

		_, err = cw.Write(in)
		So(err, ShouldBeNil)

		err = cw.Close()
		So(err, ShouldBeNil)

		Convey("Then the chunk should be read out intact", func() {
			cr, err := sp.ChunkReader("test", 0)
			So(err, ShouldBeNil)

			out, err := ioutil.ReadAll(cr)
			So(err, ShouldBeNil)
			So(out, ShouldResemble, in)

			err = cr.Close()
			So(err, ShouldBeNil)
		})
	})
}
