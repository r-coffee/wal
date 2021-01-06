package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

var (
	runningInTest = false
)

type db struct {
	directory      string
	files          map[string]*fileManager
	maxLoadedFiles uint8
}

func createDB(dir string, maxLoadedFiles uint8) (*db, error) {
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

	var d db
	d.files = make(map[string]*fileManager)

	// normalize directory
	if strings.HasSuffix(dir, "/") {
		d.directory = dir[:len(dir)-1]
	} else {
		d.directory = dir
	}

	d.maxLoadedFiles = maxLoadedFiles

	// load the files
	err := d.loadFiles()
	if err != nil {
		return nil, err
	}

	return &d, nil
}

func (d *db) storedFiles() ([]string, error) {
	var results []string

	files, err := ioutil.ReadDir(d.directory)
	if err != nil {
		return results, err
	}

	for _, x := range files {
		if x.IsDir() {
			results = append(results, x.Name())
		}
	}

	return results, nil
}

func (d *db) loadFiles() error {
	files, err := d.storedFiles()
	if err != nil {
		return err
	}

	for _, x := range files {
		fm, err := createFileManager(fmt.Sprintf("%s/%s", d.directory, x), d.maxLoadedFiles)
		if err != nil {
			return err
		}
		d.files[x] = fm
	}

	return nil
}

func (d *db) createFile(name string) error {
	dir := fmt.Sprintf("%s/%s", d.directory, name)

	_, exists := d.files[name]
	if exists {
		return nil
	}

	fm, err := createFileManager(dir, d.maxLoadedFiles)
	if err != nil {
		return err
	}

	d.files[name] = fm

	return nil
}

func (d *db) deleteFile() {
	// check if file exists

	// if not, return error

	// if so, delete
}
