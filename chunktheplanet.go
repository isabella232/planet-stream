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
	"flag"
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

type PBF struct {
	Filename string
	file *os.File
}

func Open(filename string) (PBF, error) {
	file, err := os.Open(filename)
	if err != nil {
		return PBF{}, err
	}
	return PBF{filename, file}, nil
}

func (pbf PBF) Close() error {
	return pbf.file.Close()
}

func main() {
	locPtr := flag.String("i", "", "input file")
	flag.Parse()

	fname := *locPtr
	pbf, err := Open(fname)
	check(err)
	defer pbf.Close()

	header, err := pbf.ReadFileHeader()

	fmt.Println("File:", pbf.Filename)
	fmt.Println("Source:", header.GetSource())
	fmt.Println("OsmosisReplicationBaseUrl:", header.GetOsmosisReplicationBaseUrl())
	fmt.Println("RequiredFeatures:", header.GetRequiredFeatures())
	fmt.Println("Writingprogram:", header.GetWritingprogram())
	fmt.Println("OsmosisReplicationSequenceNumber:", header.GetOsmosisReplicationSequenceNumber())

	pbf.Rewind()
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
		fname := fmt.Sprintf("chunks/%d_chunk_%s", i, path.Base(pbf.Filename))
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

func (pbf PBF) Rewind() {
	pbf.file.Seek(int64(0), 0)
}

func (pbf PBF) ReadFileHeader() (*OSMPBF.HeaderBlock, error) {

	// Start at the beginning 'cause that's where the file header
	// lives.
	pbf.Rewind()

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

func (pbf PBF) NextBlock() (*Block, error) {

	// First four bytes are a BigEndian int32 indicating the
	// size of the subsequent BlobHeader.
	sizeBlock := make([]byte, 4)
	_, err := pbf.file.Read(sizeBlock)
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBlock)

	startByte, _ := pbf.file.Seek(int64(0), 1)

	// Grab the next <size> bytes
	b := make([]byte, size)
	_, err = pbf.file.Read(b)
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
	_, err = pbf.file.Read(b)
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