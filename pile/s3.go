package pile

import (
	"bytes"
	"fmt"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/s3"
)

// s3Pile is an Amazon S3 bucket and login. It implements
// the Pile interface.
type s3Pile struct {
	svc    *s3.S3
	bucket string
}

// NewS3Pile from an AWS config and bucket name
func NewS3Pile(config *aws.Config, bucket string) Pile {
	return s3Pile{s3.New(config), bucket}
}

// ChunkReader creates an io.Reader directly for an S3 object
func (sp s3Pile) ChunkReader(name string, part int) (ChunkReader, error) {
	req, _ := sp.svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(sp.bucket),
		Key:    aws.String(objectKey(name, part)),
	})
	return &objectReader{req, nil}, nil
}

// objectReader representing an S3 object
type objectReader struct {
	req *aws.Request
	obj *s3.GetObjectOutput
}

// Read bytes from an S3 object. Make the request on
// the first call.
func (or *objectReader) Read(p []byte) (n int, err error) {
	if or.obj == nil {
		err = or.req.Send()
		or.obj = or.req.Data.(*s3.GetObjectOutput)
	}
	if err != nil {
		return
	}

	return or.obj.Body.Read(p)
}

// Close the reading S3 object
func (or *objectReader) Close() error {
	if or.obj != nil && or.obj.Body != nil {
		return or.obj.Body.Close()
	}
	return nil
}

// Last chunk if the S3 object's size is 0
func (or *objectReader) Last() bool {
	if or.obj != nil {
		return *or.obj.ContentLength == 0
	}
	return false
}

// ChunkWriter creates a chunk in the S3 bucket to be written to
func (sp s3Pile) ChunkWriter(name string, part int) (ChunkWriter, error) {
	ow := &objectWriter{
		sp:     sp,
		name:   name,
		part:   part,
		closed: false,
	}
	return ow, nil
}

// objectWriter for a new chunk in S3
type objectWriter struct {
	sp     s3Pile
	name   string
	part   int
	buf    bytes.Buffer
	closed bool
}

// Write bytes to a chunk in S3
func (ow *objectWriter) Write(p []byte) (n int, err error) {
	if ow.closed {
		panic("write to closed S3 object")
	}
	return ow.buf.Write(p)
}

// Close the S3 chunk. Due to the limitations of S3, this is when
// the chunk data is *actually* sent. It cannot be streamed
// before this.
func (ow *objectWriter) Close() error {
	if ow.closed {
		return nil
	}

	_, err := ow.sp.svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(ow.sp.bucket),
		Key:    aws.String(objectKey(ow.name, ow.part)),
		Body:   bytes.NewReader(ow.buf.Bytes()),
	})
	if err == nil {
		ow.closed = true
	}
	return err
}

// SetLast chunk. This is done by creating a subsequent chunk
// as an empty S3 object.
func (ow *objectWriter) SetLast() error {
	ch, err := ow.sp.ChunkWriter(ow.name, ow.part+1)
	if err != nil {
		return err
	}
	_, err = ch.Write(make([]byte, 0))
	return err
}

// objectKey for a chunk's name and part
func objectKey(name string, part int) string {
	return fmt.Sprintf("%s/%d", name, part)
}
