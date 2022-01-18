package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
)

const EXTERNAL_THRESHOLD int = 10_000_000
const RECORD_SIZE int = 100
const KEY_SIZE int = 10

type Record = [RECORD_SIZE]byte

func recordsFromFile(inputFile *os.File, limit int) (res []Record, err error) {
	buf := make([]byte, RECORD_SIZE)
	length := 0
	for {
		n, err := inputFile.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return res, err
		}
		if n != RECORD_SIZE {
			return res, fmt.Errorf("read partial record")
		}
		res = append(res, *(*[RECORD_SIZE]byte)(buf))
		length++
		if limit > 0 && length >= limit {
			break
		}
	}
	return res, nil
}

func sortRecords(records []Record) {
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i][:KEY_SIZE], records[j][:KEY_SIZE]) < 0
	})
}

func outputRecords(records []Record, outputFile *os.File) (err error) {
	for _, r := range records {
		if _, err := outputFile.Write(r[:]); err != nil {
			return err
		}
	}
	return nil
}

func doSort(input, output string) (err error) {
	inputFile, err := os.Open(input)
	if err != nil {
		return
	}
	defer inputFile.Close()
	ist, err := inputFile.Stat()
	if err != nil {
		return
	}
	outputFile, err := os.Create(output)
	if err != nil {
		return
	}
	defer outputFile.Close()
	if ist.Size() <= int64(EXTERNAL_THRESHOLD) {
		// sort directly
		records, err := recordsFromFile(inputFile, -1)
		if err != nil {
			return err
		}
		sortRecords(records)
		if err := outputRecords(records, outputFile); err != nil {
			return err
		}
		return nil
	}

	// external sort
	numChunks := int(math.Ceil(float64(ist.Size()) / float64(EXTERNAL_THRESHOLD)))
	if err := externalSort(numChunks, inputFile, outputFile); err != nil {
		return err
	}
	return nil
}

func externalSort(numChunks int, inputFile, outputFile *os.File) (err error) {
	// 1. sort the chunks and save
	files := make([]*os.File, numChunks)
	tmpdir, err := os.MkdirTemp(os.TempDir(), "sort-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)
	for i := 0; i < numChunks; i++ {
		records, err := recordsFromFile(inputFile, EXTERNAL_THRESHOLD/RECORD_SIZE)
		if err != nil {
			return err
		}
		sortRecords(records)
		fileName := fmt.Sprintf("sort-%d-*", i)
		tmpf, err := os.CreateTemp(tmpdir, fileName)
		if err != nil {
			return err
		}
		if err := outputRecords(records, tmpf); err != nil {
			return err
		}
		files[i] = tmpf
	}
	for _, f := range files {
		if _, err := f.Seek(0, 0); err != nil {
			return err
		}
	}

	// 2. merge the chunks
	mergeSize := EXTERNAL_THRESHOLD / (numChunks + 1)
	chunks := make([][]Record, numChunks)
	for i := range chunks {
		records, err := recordsFromFile(files[i], mergeSize)
		if err != nil {
			return err
		}
		chunks[i] = records
	}
	buf := make([]Record, 0, mergeSize)
	pq := make(PriorityQueue, 0)
	for i, chunk := range chunks {
		if len(chunk) > 0 {
			heap.Push(&pq, &Item{
				record:       chunk[0],
				nextPosition: 1,
				chunkIdx:     i,
			})
		}
	}
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		if len(buf) >= cap(buf) {
			if err := outputRecords(buf, outputFile); err != nil {
				return err
			}
			buf = buf[:0]
		}
		buf = append(buf, item.record)
		if item.nextPosition >= len(chunks[item.chunkIdx]) {
			// load more data to chunks[item.chunkIdx]
			records, err := recordsFromFile(files[item.chunkIdx], mergeSize)
			if err != nil {
				return err
			}
			chunks[item.chunkIdx] = records
			item.nextPosition = 0
		}
		if item.nextPosition < len(chunks[item.chunkIdx]) {
			heap.Push(&pq, &Item{
				record:       chunks[item.chunkIdx][item.nextPosition],
				nextPosition: item.nextPosition + 1,
				chunkIdx:     item.chunkIdx,
			})
		}
	}
	if err := outputRecords(buf, outputFile); err != nil {
		return err
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}

	input := os.Args[1]
	output := os.Args[2]
	log.Printf("Sorting %s to %s\n", input, output)
	if err := doSort(input, output); err != nil {
		log.Fatal(err)
	}
}

// heap implementation
type Item struct {
	record       Record
	nextPosition int
	chunkIdx     int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int      { return len(pq) }
func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq PriorityQueue) Less(i, j int) bool {
	return bytes.Compare(pq[i].record[:KEY_SIZE], pq[j].record[:KEY_SIZE]) < 0
}

func (pq *PriorityQueue) Push(x interface{}) { *pq = append(*pq, x.(*Item)) }
func (pq *PriorityQueue) Pop() interface{} {
	n := len(*pq)
	item := (*pq)[n-1]
	(*pq)[n-1] = nil // avoid memory leak
	*pq = (*pq)[0 : n-1]
	return item
}
