// Works with the Protocol Buffer Binary Format (PBF) of OpenStreetMap
// planetfiles to break up a large file into many smaller, standalone files
// that can be used like their larger counterparts for import and other data
// pipeline uses.
//
// This can greatly speed up planetfile import, as it allows for multiple
// chunks to be imported through several different threads or on different
// machines running parallel operations.
//
// Additionally, this package provides multiple streaming modes for remote
// servers which provide HTTP Range support, allowing for data to be
// imported concurrently with its transfer.
//
// This package only deals with PBFs at the Block level, and does not
// directly interact with or modify more sophisticated datatypes of the
// OSMPBF format corresponding to the map data itself, such as
// PrimitiveGroups, Nodes, Ways, or Relations.
//
// If you'd like to interact with the map-related data using Go, you
// can do so using the OSMPBF library:
// http://github.com/qedus/osmpbf
//
// For more information on the Protocol Buffer Binary Format:
// https://wiki.openstreetmap.org/wiki/PBF_Format
package streams

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	"github.com/qedus/osmpbf/OSMPBF"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

// Generic interface for Streams; by default they keep track of their last
// read position (Cursor) after Reads.
//
// Planetfile methods rely on this interface for its streams in order to
// work properly for various use cases.
type Stream interface {
	Close() error
	ReadRange(int64, []byte) (int, error)
}

// RateLimiter provides a generalized interface for limiting the rate at which
// requests are made.
type RateLimiter struct {
	LastRequest time.Time
	RequestRate time.Duration
}

// Sleeps if the last request has happened sooner than the current rate between
// requests. Once done sleeping, it will automatically update the lastRequest
// time.
func (r *RateLimiter) limit() {
	if r.RequestRate > 0 {
		start := r.LastRequest
		now := time.Now()
		elapsed := now.Sub(start)
		if elapsed < r.RequestRate {
			time.Sleep((r.RequestRate - elapsed))
		}
	}
	r.LastRequest = time.Now()
}

// Opens a Stream wrapped in a Planetfile interface based on the protocol
// specified in the location string.
func Open(loc string) (*Planetfile, error) {

	var e error
	var s Stream

	if strings.HasPrefix(loc, "http") {
		s, e = OpenHttp(loc)
	} else if strings.HasPrefix(loc, "s3://") {
		s, e = OpenS3(loc)
	} else {
		s, e = OpenIO(loc)
	}

	if e != nil {
		return &Planetfile{}, e
	}
	pbf := &Planetfile{
		Location: loc,
		Stream:   s,
	}
	// Peek at the header block to make sure it's readable and
	// the right format; we're not interested in what it contains.
	_, e = pbf.ReadFileHeader()
	return pbf, nil
}

// IOStream attaches to a file location.
type IOStream struct {
	Location string
	abspath  string
}

// Creates a new IOStream and attaches it to the specified file location.
func OpenIO(filename string) (*IOStream, error) {
	abs, err := filepath.Abs(filename)
	if err != nil {
		return &IOStream{}, err
	}
	return &IOStream{filename, abs}, nil
}

// Does nothing; provided for the interface.
func (s IOStream) Close() error {
	return nil
}

// Fills the passed in byte array with the appropriate length of content from
// the current cursor position of the file, incrementing the cursor when
// complete.
//
// The return value is the number of bytes read.
func (s IOStream) ReadRange(start int64, buff []byte) (int, error) {
	file, err := os.Open(s.Location)
	defer file.Close()
	if err != nil {
		return 0, err
	}
	_, err = file.Seek(start, 0)
	if err != nil {
		return 0, err
	}
	return file.Read(buff)
}

// HTTP Streaming
type HttpStream struct {
	Location    string
	cur         int64
	client      *http.Client
	RateLimiter *RateLimiter
}

// Opens a URL for streaming.
//
// Should work with HTTP as well as HTTPS, but not on servers that don't
// support byte Range headers.
func OpenHttp(url string) (*HttpStream, error) {
	return &HttpStream{url, 0, &http.Client{}, &RateLimiter{time.Now(), 0 * time.Second}}, nil
}

// Makes a partial HTTP request using the Range header for the specified number of bytes
// from the current read cursor.
//
// NB Range requests only work for servers which respond to byte range headers.
func (s *HttpStream) ReadRange(start int64, buff []byte) (int, error) {
	s.RateLimiter.limit()
	end := start + int64(len(buff))
	req, _ := http.NewRequest("GET", s.Location, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	res, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	// If the HTTP server returns an error status code, it'll still count
	// as a successful request by the http library.
	//
	// So we also have to make sure that the HTTP status code is 2xx.
	if res.StatusCode <= 200 || res.StatusCode >= 299 {
		if res.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			return 0, io.EOF
		}
		return 0, errors.New(res.Status)
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return 0, err
	}
	copy(buff, b)
	res.Body.Close()
	return len(buff), nil
}

// Doesn't actually do anything, as client requests are closed after
// taking place.
//
// Provided to fulfill interface requirements.
func (s *HttpStream) Close() error {
	return nil
}

// S3 Streaming
type S3Stream struct {
	Location    string
	Bucket      string
	Key         string
	service     *s3.S3
	cur         int64
	RateLimiter *RateLimiter
}

// Opens an S3 bucket for streaming.
//
// The URL should be provided in s3://bucket/path/key format.
func OpenS3(loc string) (*S3Stream, error) {
	// Get the Bucket and Key
	if strings.HasPrefix(loc, "s3://") == false {
		return &S3Stream{}, fmt.Errorf("Url must start with s3://; Got %s", loc)
	}
	p := strings.Split(loc[5:], "/")
	bucket := strings.Join(p[0:len(p)-1], "/") + "/"
	key := p[len(p)-1]

	// Create the AWS session and S3Stream
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	stream := &S3Stream{
		Location:    loc,
		Bucket:      bucket,
		Key:         key,
		cur:         0,
		service:     svc,
		RateLimiter: &RateLimiter{time.Now(), 0 * time.Second},
	}

	return stream, nil
}

// Makes a partial request to the S3 service to retrieve the specified number
// of bytes, starting at the current read position.
func (s *S3Stream) ReadRange(start int64, buff []byte) (int, error) {
	s.RateLimiter.limit()
	end := start + int64(len(buff))
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}
	res, err := s.service.GetObject(params)
	if err != nil {
		reqErr := err.(awserr.RequestFailure)
		if reqErr.StatusCode() == http.StatusRequestedRangeNotSatisfiable {
			return 0, io.EOF
		}
		return 0, err
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return 0, err
	}
	copy(buff, b)
	res.Body.Close()

	return len(buff), nil
}

// Doesn't actually do anything.
//
// Provided to fulfill interface requirements.
func (s *S3Stream) Close() error {
	return nil
}

// Planetfile provides high-level interface to the specific streaming
// functionality.
type Planetfile struct {
	Location string
	Stream   Stream
}

// Calls the underlying Stream's Close function. Depending on the type of
// Stream, this may not have any effect, but it's still a good thing to
// do.
func (pbf Planetfile) Close() {
	pbf.Stream.Close()
}

// Copies and decodes the first block of the Stream, which happens to
// be a HeaderBlock.
//
// You can use this to dump metadata about a file, such as its origin and
// replication sequence number.
func (pbf Planetfile) ReadFileHeader() (*OSMPBF.HeaderBlock, error) {

	block, err := pbf.GetBlock(0)
	if err != nil {
		return nil, err
	}

	data, err := block.Decode()
	if err != nil {
		return nil, err
	}

	header := new(OSMPBF.HeaderBlock)
	err = proto.Unmarshal(data, header)

	return header, err

}

func (pbf Planetfile) GetBlock(startByte int64) (*Block, error) {

	p := startByte

	// First four bytes are a BigEndian int32 indicating the
	// size of the subsequent BlobHeader.
	sizeBlock := make([]byte, 4)
	l, err := pbf.Stream.ReadRange(startByte, sizeBlock)
	if err != nil {
		return nil, err
	}

	// Increment the byte cursor.
	p += int64(l)

	size := binary.BigEndian.Uint32(sizeBlock)

	// Grab the next <size> bytes
	b := make([]byte, size)
	l, err = pbf.Stream.ReadRange(p, b)
	if err != nil {
		return nil, err
	}

	// Increment the byte cursor again.
	p += int64(l)

	// Unmarshal the Protobuf structure
	blobHeader := new(OSMPBF.BlobHeader)
	err = proto.Unmarshal(b, blobHeader)
	if err != nil {
		return nil, err
	}

	block := &Block{
		pbf:        pbf,
		DataType:   blobHeader.GetType(),
		BlockStart: startByte,
		BlobStart:  p,
		BlockEnd:   p + int64(blobHeader.GetDatasize()),
	}
	return block, err

}

// Blocks are the main file primitive of PBF-based Planetfiles.
//
// This package deals with two specific types of Blocks, HeaderBlocks
// and Blobs.
//
// Only one HeaderBlock should exist in the entire file, and it should
// be the first block in the file. The rest are all Blobs, which
// themselves contain data which can be subsequently unmarshalled into
// more specific data types.
//
// For the purposes of this package, however, we only deal with the data
// at the Block level.
type Block struct {
	pbf        Planetfile
	DataType   string
	BlockStart int64
	BlobStart  int64
	BlockEnd   int64
}

// Dumps the entire block to a byte array.
//
// Useful if you're going to be using a particular block many times,
// for example the HeaderBlock, and don't want to make multiple
// requests to retreive the data from the source every time it's
// needed.
func (b Block) Dump() ([]byte, error) {
	buff := make([]byte, b.BlockEnd-b.BlockStart)
	_, err := b.pbf.Stream.ReadRange(b.BlockStart, buff)
	return buff, err
}

// Dumps the entire Block to the indicated file location, including its
// leading size delimiter and BlobHeader.
func (b Block) Write(f io.Writer) (int, error) {
	buff := make([]byte, b.BlockEnd-b.BlockStart)
	_, err := b.pbf.Stream.ReadRange(b.BlockStart, buff)
	if err != nil {
		return 0, err
	}
	size, err := f.Write(buff)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Reads the appropriate range in the Stream to obtain the Blob data and
// then unmarshals it.
func (b Block) GetBlob() (*OSMPBF.Blob, error) {
	data := make([]byte, b.BlockEnd-b.BlobStart)
	b.pbf.Stream.ReadRange(b.BlobStart, data)
	blob := new(OSMPBF.Blob)
	err := proto.Unmarshal(data, blob)
	if err != nil {
		return blob, err
	}
	return blob, nil
}

// Decodes the raw data of the Block so that it can be unmarshaled.
//
// Sometimes the data is gzipped, in which case it will be decompressed
// before being returned.
//
// (Taken directly from qedus/osmpbf.)
func (b Block) Decode() ([]byte, error) {
	blob, err := b.GetBlob()
	if err != nil {
		return make([]byte, 0), err
	}
	// All of this is taken directly from github.com/qedus/osmpbf/OSMPBF
	switch {
	case blob.Raw != nil:
		return blob.GetRaw(), nil

	case blob.ZlibData != nil:
		r, err := zlib.NewReader(bytes.NewReader(blob.GetZlibData()))
		if err != nil {
			return nil, err
		}
		buf := bytes.NewBuffer(make([]byte, 0, blob.GetRawSize()+bytes.MinRead))
		_, err = buf.ReadFrom(r)
		if err != nil {
			return nil, err
		}
		if buf.Len() != int(blob.GetRawSize()) {
			err = fmt.Errorf("raw blob data size %d but expected %d", buf.Len(), blob.GetRawSize())
			return nil, err
		}
		return buf.Bytes(), nil

	default:
		return nil, errors.New("unknown blob data")
	}
}
