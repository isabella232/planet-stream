package main

import (
	"flag"
	"fmt"
	"github.com/mapbox/planet-stream/streams"
	"log"
	"os"
	"path"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

// Example of stream library usage for a command-line utility.
//
// This takes an input file (or URL or S3 location) as -i and creates a series
// of chunked files to the ./chunks directory, each of which can be imported
// through osmosis independently.
func main() {
	locPtr := flag.String("i", "", "input file or URL")
	flag.Parse()

	fname := *locPtr
	pbf, err := streams.Open(fname)
	check(err)
	defer pbf.Close()

	header, err := pbf.ReadFileHeader()

	fmt.Println("File:", pbf.Location)
	fmt.Println("Source:", header.GetSource())
	fmt.Println("OsmosisReplicationBaseUrl:", header.GetOsmosisReplicationBaseUrl())
	fmt.Println("RequiredFeatures:", header.GetRequiredFeatures())
	fmt.Println("Writingprogram:", header.GetWritingprogram())
	fmt.Println("OsmosisReplicationSequenceNumber:", header.GetOsmosisReplicationSequenceNumber())

	headBlock, err := pbf.GetBlock(0)
	check(err)

	headData, err := headBlock.Dump()
	check(err)

	fmt.Printf("Total size: %d bytes\n", pbf.TotalSize)
	p := headBlock.BlockEnd

	fmt.Printf("\n")
	var file *os.File
	i := 0
	for {
		if p >= pbf.TotalSize {
			break
		}
		block, err := pbf.GetBlock(p)
		check(err)
		p = block.BlockEnd
		fname := fmt.Sprintf("chunks/%d_chunk_%s", i, path.Base(pbf.Location))
		file, err = os.Create(fname)
		check(err)
		_, err = file.Write(headData)
		check(err)
		block.Write(file)
		file.Close()
		fmt.Printf("\r")
		fmt.Printf("Processing... %s           ", humanBytes(block.BlockStart))
		i++
	}

	fmt.Printf("\nProcessed size: %d bytes\n", p)

	fmt.Printf("Processing complete; %d chunks total.\n", i)

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
