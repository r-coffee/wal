package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type fileManager struct {
	dir            string
	loadedFiles    map[string]*file
	masterIndex    uint64
	minIndex       uint64
	mux            sync.Mutex
	maxLoadedFiles uint8
}

func createFileManager(dir string, maxLoadedFiles uint8) (*fileManager, error) {
	// ensure directory exists
	if fileExists(dir) == false {
		var err error
		if runningInTest {
			err = os.Mkdir(dir, 0777)
		} else {
			err = os.Mkdir(dir, 0660)
		}
		if err != nil {
			return nil, err
		}
	}

	var fm fileManager
	if strings.HasSuffix(dir, "/") {
		fm.dir = dir[:len(dir)-1]
	} else {
		fm.dir = dir
	}
	fm.maxLoadedFiles = maxLoadedFiles

	m := make(map[string]*file)
	fm.loadedFiles = m

	// load master index
	sortedFiles := fm.sortedFiles()
	if len(sortedFiles) > 0 {
		latest := sortedFiles[len(sortedFiles)-1]
		f, err := os.Open(latest + ".map")
		if err != nil {
			log.Printf("cannot open latest: %v", err)
			return nil, err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Split(bufio.ScanLines)
		var biggest uint64
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "|")
			idx, _ := strconv.ParseUint(parts[0], 10, 64)
			if idx > biggest {
				biggest = idx
			}
		}
		fm.masterIndex = biggest + 1
		log.Printf("resuming with offset %d", fm.masterIndex)
	}

	// load minIndex
	if len(sortedFiles) > 0 {
		first := sortedFiles[0]
		f, err := os.Open(first + ".map")
		if err != nil {
			log.Printf("cannot open first: %v", err)
			return nil, err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Split(bufio.ScanLines)
		if scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "|")
			idx, _ := strconv.ParseUint(parts[0], 10, 64)
			fm.minIndex = idx
		}
	}

	return &fm, nil
}

// return the offset we wrote if successfull
func (fm *fileManager) append(dat []byte) (uint64, error) {
	fm.mux.Lock()
	defer fm.mux.Unlock()

	// construct filename
	fname := fmt.Sprintf("%s/%s", fm.dir, idxToFName(fm.masterIndex))

	// check if file is already loaded
	f, exists := fm.loadedFiles[fname]
	if exists {
		err := f.append(fm.masterIndex, dat)
		if err != nil {
			return 0, err
		}
		fm.masterIndex++
		return fm.masterIndex - 1, nil
	}

	ff, err := createOrLoadFile(fname)
	if err != nil {
		return 0, err
	}
	fm.loadedFiles[fname] = ff
	err = ff.append(fm.masterIndex, dat)
	if err != nil {
		return 0, err
	}
	fm.masterIndex++

	// unload files
	if len(fm.loadedFiles) > int(fm.maxLoadedFiles) {
		// find oldest file that is still open
		sortedFiles := fm.sortedFiles()
		for _, x := range sortedFiles {
			fff, exists := fm.loadedFiles[x]
			if exists {
				fff.close()
				delete(fm.loadedFiles, sortedFiles[0])
				break
			}
		}
	}

	return fm.masterIndex - 1, nil
}

func idxToFName(idx uint64) string {
	if idx == 0 {
		return fmt.Sprintf("%017d.dat", 0)
	}

	n := uint64(math.Floor(float64(idx) / 1000))
	return fmt.Sprintf("%017d.dat", n)
}

func (fm *fileManager) sortedFiles() []string {
	var results []string
	files, err := ioutil.ReadDir(fm.dir)
	if err != nil {
		log.Printf("cannot read files for %s: %v", fm.dir, err)
		return results
	}

	for _, x := range files {
		if x.IsDir() == false {
			if strings.HasSuffix(x.Name(), ".dat") {
				results = append(results, fmt.Sprintf("%s/%s", fm.dir, x.Name()))
			}
		}
	}

	sort.Strings(results)
	return results
}

func (fm *fileManager) purge() {
	fm.mux.Lock()
	defer fm.mux.Unlock()

	files := fm.sortedFiles()
	for _, x := range files {
		os.Remove(x)
		os.Remove(x + ".map")
		delete(fm.loadedFiles, x)
	}

	fm.masterIndex = 0
}

func (fm *fileManager) read(idx uint64) ([]byte, error) {
	if idx < fm.minIndex || idx > fm.masterIndex {
		return []byte{}, errors.New("invalid index")
	}

	fm.mux.Lock()
	defer fm.mux.Unlock()

	// construct filename
	fname := fmt.Sprintf("%s/%s", fm.dir, idxToFName(idx))

	// check if file is already loaded
	f, exists := fm.loadedFiles[fname]
	if exists {
		return f.read(idx)
	}

	if fileExists(fname) == false {
		return []byte{}, errors.New("index does not exist")
	}

	// load file
	ff, err := createOrLoadFile(fname)
	if err != nil {
		return []byte{}, err
	}
	fm.loadedFiles[fname] = ff

	// read data
	return ff.read(idx)
}
