package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
)

type file struct {
	name          string
	mux           sync.Mutex
	lookup        map[uint64]fileEntry
	currentOffset uint64
	hDat          *os.File
	wDat          *bufio.Writer
	hMap          *os.File
	wMap          *bufio.Writer
	cache         []byte
}

type fileEntry struct {
	size   uint16
	offset uint64
}

func createOrLoadFile(name string) (*file, error) {
	// look for existing file
	if fileExists(name) {
		return loadFile(name)
	}

	var newFile file
	newFile.name = name
	newFile.lookup = make(map[uint64]fileEntry)

	// load file handles
	fh, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	newFile.wDat = bufio.NewWriter(fh)
	newFile.hDat = fh

	fh2, err := os.OpenFile(name+".map", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	newFile.wMap = bufio.NewWriter(fh2)
	newFile.hMap = fh2

	return &newFile, nil
}

// return true if file should split
func (f *file) append(idx uint64, dat []byte) error {
	f.mux.Lock()
	defer f.mux.Unlock()

	bytesWritten, err := f.wDat.Write(dat)
	if err != nil {
		return err
	}
	err = f.wDat.Flush()
	if err != nil {
		return err
	}

	f.cache = append(f.cache, dat...)

	// write to log
	err = f.appendLog(idx, uint16(bytesWritten))
	if err != nil {
		return err
	}

	// update lookup map
	var fe fileEntry
	fe.size = uint16(bytesWritten)
	fe.offset = f.currentOffset
	f.lookup[idx] = fe
	f.currentOffset += uint64(bytesWritten)

	return nil
}

func (f *file) appendLog(idx uint64, size uint16) error {
	_, err := f.wMap.WriteString(fmt.Sprintf("%d|%d\n", idx, size))
	if err != nil {
		return err
	}
	return f.wMap.Flush()
}

func fileExists(name string) bool {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return false
	}

	return true
}

func loadLookup(name string) (map[uint64]fileEntry, uint64, error) {
	dat := make(map[uint64]fileEntry)
	f, err := os.Open(name)
	if err != nil {
		return dat, 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var offset uint64
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "|")
		idx, _ := strconv.ParseUint(parts[0], 10, 64)
		o, _ := strconv.ParseUint(parts[1], 10, 16)
		var fe fileEntry
		fe.size = uint16(o)
		fe.offset = offset
		offset += o
		dat[idx] = fe
	}

	return dat, offset, nil
}

func loadFile(name string) (*file, error) {
	if fileExists(name) == false {
		return nil, errors.New("file does not exist")
	}

	var f file
	f.name = name
	raw, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}
	f.cache = raw

	// load file handles
	fh, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	f.wDat = bufio.NewWriter(fh)
	f.hDat = fh

	fh2, err := os.OpenFile(name+".map", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	f.wMap = bufio.NewWriter(fh2)
	f.hMap = fh2

	// re-construct lookup map
	l, offset, err := loadLookup(name + ".map")
	if err != nil {
		return nil, err
	}
	f.lookup = l
	f.currentOffset = offset

	return &f, nil
}

func (f *file) read(idx uint64) ([]byte, error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	fe, exists := f.lookup[idx]
	if exists == false {
		return []byte{}, errors.New("invalid index")
	}

	start := int(fe.offset)
	stop := start + int(fe.size)
	return f.cache[start:stop], nil
}

func (f *file) close() {
	if f.wDat != nil {
		f.wDat.Flush()
		f.hDat.Close()
	}

	if f.wMap != nil {
		f.wMap.Flush()
		f.hMap.Close()
	}
}
