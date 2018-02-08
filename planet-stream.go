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
package main

import (
	"log"
	"os"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/qedus/osmpbf/OSMPBF"
	"encoding/binary"
	"compress/zlib"
	"bytes"
	"errors"
	"path"
	"io"
	"io/ioutil"
	"flag"
	"net/http"
	"strings"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	locPtr := flag.String("i", "", "input file or URL")
	flag.Parse()

	fname := *locPtr
	pbf, err := OpenStream(fname)
	check(err)
	defer pbf.Close()

	header, err := pbf.ReadFileHeader()

	fmt.Println("File:", pbf.Location)
	fmt.Println("Source:", header.GetSource())
	fmt.Println("OsmosisReplicationBaseUrl:", header.GetOsmosisReplicationBaseUrl())
	fmt.Println("RequiredFeatures:", header.GetRequiredFeatures())
	fmt.Println("Writingprogram:", header.GetWritingprogram())
	fmt.Println("OsmosisReplicationSequenceNumber:", header.GetOsmosisReplicationSequenceNumber())

	pbf.stream.Rewind()
	headBlock, err := pbf.NextBlock()
	check(err)

	fmt.Printf("\n")
	var file *os.File
	i := 0
	for {
		block, err := pbf.NextBlock()
		if err == io.EOF {
			break
		}
		check(err)
		fname := fmt.Sprintf("chunks/%d_chunk_%s", i, path.Base(pbf.Location))
		file, err = os.Create(fname)
		check(err)
		headBlock.Write(file)
		block.Write(file)
		file.Close()
		fmt.Printf("\r")
		fmt.Printf("Processing... %s           ", humanBytes(block.StartByte))
		i++
	}

	fmt.Printf("Processing complete; %d chunks total.\n", i)

}

// Generic interface for Streams; by default they keep track of their last
// read position (Cursor) after Reads.
//
// Planetfile methods rely on this interface for its streams in order to
// work properly for various use cases.
type Stream interface {
	Close() error
	Read([]byte) (int, error)
	Rewind()
	SetCursor(int64) (error)
	GetCursor() (int64, error)
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
			time.Sleep((r.RequestRate-elapsed))
		}
	}
	r.LastRequest = time.Now()
}


// Opens a Stream wrapped in a Planetfile interface based on the protocol
// specified in the location string.
func OpenStream(loc string) (*Planetfile, error) {
	if strings.HasPrefix(loc, "http") {
		s, e := OpenHttp(loc)
		return &Planetfile{loc, s}, e
	}
	if strings.HasPrefix(loc, "s3://") {
		s, e := OpenS3(loc)
		return &Planetfile{loc, s}, e
	}
	s, e := OpenIO(loc)
	return &Planetfile{loc, s}, e
}

// IOStream attaches to a file location.
type IOStream struct {
	Location string
	file *os.File
}

// Creates a new IOStream and attaches it to the specified file location.
func OpenIO(filename string) (*IOStream, error) {
	file, err := os.Open(filename)
	if err != nil {
		return &IOStream{}, err
	}
	return &IOStream{filename, file}, nil
}

// Closes the file attached to this Stream.
func (s IOStream) Close() error {
	return s.file.Close()
}

// Fills the passed in byte array with the appropriate length of content from
// the current cursor position of the file, incrementing the cursor when
// complete.
//
// The return value is the number of bytes read.
func (s IOStream) Read(buff []byte) (int, error) {
	return s.file.Read(buff)
}

// Sets the current read position to the beginning of the file.
func (s IOStream) Rewind() {
	s.SetCursor(0)
}

// Returns the current read position.
func (s IOStream) GetCursor() (int64, error) {
	return s.file.Seek(0, 1)
}

// Sets the current read position.
func (s IOStream) SetCursor(i int64) (error) {
	_, e := s.file.Seek(i, 0)
	return e
}

// HTTP Streaming
type HttpStream struct {
	Location string
	cur int64
	client *http.Client
	RateLimiter *RateLimiter
}

// Opens a URL for streaming.
//
// Should work with HTTP as well as HTTPS, but not on servers that don't
// support byte Range headers.
func OpenHttp(url string) (*HttpStream, error) {
	return &HttpStream{url, 0, &http.Client{}, &RateLimiter{time.Now(), 0*time.Second}}, nil
}

// Makes a partial HTTP request using the Range header for the specified number of bytes
// from the current read cursor.
//
// NB Range requests only work for servers which respond to byte range headers.
func (s *HttpStream) Read(buff []byte) (int, error) {
	s.RateLimiter.limit()
	end := s.cur + int64(len(buff))
	req, _ := http.NewRequest("GET", s.Location, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", s.cur, end))
	s.SetCursor(end)
	res, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	// Stacked defers are LIFO: Last In, First Out, so these two lines
	// will actually run backwards.
	defer res.Body.Close()
	defer io.Copy(ioutil.Discard, res.Body)
	return res.Body.Read(buff)
}

// Sets the current read position to the beginning of the file.
func (s *HttpStream) Rewind() {
	s.cur = int64(0)
}

// Returns the current read position.
func (s *HttpStream) GetCursor() (int64, error) {
	return s.cur, nil
}

// Sets the current read position.
func (s *HttpStream) SetCursor(i int64) (error) {
	s.cur = i
	return nil
}

// Doesn't actually do anything, as client requests are closed after
// taking place.
//
// Provided to fulfill interface requirements.
func (s *HttpStream) Close() (error) {
	return nil
}

// S3 Streaming
type S3Stream struct {
	Location string
	Bucket string
	Key string
	service *s3.S3
	cur int64
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
	bucket := strings.Join(p[0:len(p)-1], "/")+"/"
	key := p[len(p)-1]

	// Create the AWS session and S3Stream
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	stream := &S3Stream{
		Location: loc,
		Bucket: bucket,
		Key: key,
		cur: 0,
		service: svc,
		RateLimiter: &RateLimiter{time.Now(), 0*time.Second},
	}

	return stream, nil
}

// Makes a partial request to the S3 service to retrieve the specified number
// of bytes, starting at the current read position.
func (s *S3Stream) Read(buff []byte) (int, error) {
	s.RateLimiter.limit()
	end := s.cur + int64(len(buff))
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", s.cur, end)),
	}
	s.SetCursor(end)
	res, err := s.service.GetObject(params)
	if err != nil {
		return 0, err
	}
	res.Body.Read(buff)
	defer res.Body.Close()
	defer io.Copy(ioutil.Discard, res.Body)
	return len(buff), nil
}

// Sets the current read position to the beginning of the file.
func (s *S3Stream) Rewind() {
	s.cur = int64(0)
}

// Returns the current read position.
func (s *S3Stream) GetCursor() (int64, error) {
	return s.cur, nil
}

// Sets the current read position.
func (s *S3Stream) SetCursor(i int64) (error) {
	s.cur = i
	return nil
}

// Doesn't actually do anything.
//
// Provided to fulfill interface requirements.
func (s *S3Stream) Close() (error) {
	return nil
}

// Planetfile provides high-level interface to the specific streaming
// functionality.
type Planetfile struct {
	Location string
	stream Stream
}

// Calls the underlying Stream's Close function. Depending on the type of
// Stream, this may not have any effect, but it's still a good thing to
// do.
func (pbf Planetfile) Close() {
	pbf.stream.Close()
}

// Copies and decodes the first block of the Stream, which happens to
// be a HeaderBlock.
//
// You can use this to dump metadata about a file, such as its origin and
// replication sequence number.
func (pbf Planetfile) ReadFileHeader() (*OSMPBF.HeaderBlock, error) {

	c, _ := pbf.stream.GetCursor()

	// Start at the beginning 'cause that's where the file header
	// lives.
	pbf.stream.Rewind()

	block, err := pbf.NextBlock()
	if err != nil {
		return nil, err
	}

	data, err := block.Decode()
	if err != nil {
		return nil, err
	}

	header := new(OSMPBF.HeaderBlock)
	err = proto.Unmarshal(data, header)

	pbf.stream.SetCursor(c)

	return header, err

}

func (pbf Planetfile) NextBlock() (*Block, error) {

	// First four bytes are a BigEndian int32 indicating the
	// size of the subsequent BlobHeader.
	sizeBlock := make([]byte, 4)
	_, err := pbf.stream.Read(sizeBlock)
	if err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(sizeBlock)

	startByte, _ := pbf.stream.GetCursor()

	// Grab the next <size> bytes
	b := make([]byte, size)
	_, err = pbf.stream.Read(b)
	if err != nil {
		return nil, err
	}

	// Unmarshal the Protobuf structure
	blobHeader := new(OSMPBF.BlobHeader)
	err = proto.Unmarshal(b, blobHeader)
	if err != nil {
		return nil, err
	}

	// Grab the blob size indicated by the blobHeader.
	b = make([]byte, blobHeader.GetDatasize())
	_, err = pbf.stream.Read(b)
	if err != nil {
		return nil, err
	}

	// Unmarshal the Blob structure
	blob := new(OSMPBF.Blob)
	err = proto.Unmarshal(b, blob)
	if err != nil {
		return nil, err
	}

	block := &Block{
		DataType: blobHeader.GetType(),
		Size: blob.GetRawSize(),
		Blob: blob,
		BlobHeader: blobHeader,
		SizeBlock: sizeBlock,
		StartByte: startByte,
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
	Data []byte
	DataType string
	Size int32
	Blob *OSMPBF.Blob
	BlobHeader *OSMPBF.BlobHeader
	SizeBlock []byte
	StartByte int64
}

// Dumps the entire Block to the indicated file location, including its
// leading size delimiter and BlobHeader.
func (b Block) Write(f *os.File) int {
	f.Write(b.SizeBlock)
	data, err := proto.Marshal(b.BlobHeader)
	check(err)
	_, err = f.Write(data)
	check(err)
	data, err = proto.Marshal(b.Blob)
	check(err)
	size, err := f.Write(data)
	check(err)
	return size
}

// Decodes the raw data of the Block so that it can be demarshaled.
//
// Sometimes the data is gzipped, in which case it will be decompressed
// before being returned.
//
// (Taken directly from qedus/osmpbf.)
func (b Block) Decode() ([]byte, error) {
	blob := b.Blob
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

func humanBytes(s int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	n := float64(s)
	i := 0
	for {
		if n < 1024 {
			break
		}
		i++
		n = n / 1024
	}
	return fmt.Sprintf("%0.2f %s", n, units[i])
}