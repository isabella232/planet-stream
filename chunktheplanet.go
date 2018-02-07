package main

import (
	"log"
	"os"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/yuffster/chunktheplanet/OSMPBF"  // not a real URL
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
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

type Block struct {
	Data []byte
	DataType string
	Size int32
	Blob *OSMPBF.Blob
	BlobHeader *OSMPBF.BlobHeader
	SizeBlock []byte
	StartByte int64
}

type Stream interface {
	Close() error
	Read([]byte) (int, error)
	Rewind()
	SetCursor(int64) (error)
	GetCursor() (int64, error)
}

type RateLimiter struct {
	lastRequest time.Time
	requestRate time.Duration
}

// File IO
type IOStream struct {
	Location string
	file *os.File
}

// HTTP Streaming
type HttpStream struct {
	Location string
	cur int64
	client *http.Client
	RateLimiter *RateLimiter
}

type Planetfile struct {
	Location string
	stream Stream
}

func OpenIO(filename string) (*IOStream, error) {
	file, err := os.Open(filename)
	if err != nil {
		return &IOStream{}, err
	}
	return &IOStream{filename, file}, nil
}

func OpenHttp(url string) (*HttpStream, error) {
	return &HttpStream{url, 0, &http.Client{}, &RateLimiter{time.Now(), 0*time.Second}}, nil
}

func OpenStream(loc string) (*Planetfile, error) {
	if strings.HasPrefix(loc, "http") {
		s, e := OpenHttp(loc)
		return &Planetfile{loc, s}, e
	}
	s, e := OpenIO(loc)
	return &Planetfile{loc, s}, e
}

func (s IOStream) Close() error {
	return s.file.Close()
}

func (s IOStream) Read(buff []byte) (int, error) {
	return s.file.Read(buff)
}

func (s IOStream) Rewind() {
	s.SetCursor(0)
}

func (s IOStream) GetCursor() (int64, error) {
	return s.file.Seek(0, 1)
}

func (s IOStream) SetCursor(i int64) (error) {
	_, e := s.file.Seek(i, 0)
	return e
}

func (pbf Planetfile) Close() {
	pbf.stream.Close()
}

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

func (s *HttpStream) Rewind() {
	s.cur = int64(0)
}

func (s *HttpStream) GetCursor() (int64, error) {
	return s.cur, nil
}

func (s *HttpStream) SetCursor(i int64) (error) {
	s.cur = i
	return nil
}

func (s *HttpStream) Close() (error) {
	return nil
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

func (pbf Planetfile) ReadFileHeader() (*OSMPBF.HeaderBlock, error) {

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

func (r *RateLimiter) limit() {
	if r.requestRate > 0 {
		start := r.lastRequest
		now := time.Now()
		elapsed := now.Sub(start)
		if elapsed < r.requestRate {
			time.Sleep((r.requestRate-elapsed))
		}
	}
	r.lastRequest = time.Now()
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