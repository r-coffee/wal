package wal

import (
	"log"
	"testing"
)

func TestIdxToFName(t *testing.T) {
	var testCases = map[uint64]string{
		0:             "00000000000000000.dat",
		1:             "00000000000000000.dat",
		999:           "00000000000000000.dat",
		1000:          "00000000000000001.dat",
		1001:          "00000000000000001.dat",
		10000:         "00000000000000010.dat",
		10001:         "00000000000000010.dat",
		1000000000001: "00000001000000000.dat",
	}

	for k, v := range testCases {
		expected := v
		got := idxToFName(k)
		if expected != got {
			t.Fatalf("test: %d expected: %s, got: %s", k, expected, got)
		}
	}
}

func TestFileManagerLifeCycle(t *testing.T) {
	runningInTest = true

	fm, err := createFileManager("test", 2)
	if err != nil {
		t.Fatalf("error creating fileManager: %v", err)
	}

	// check topic name
	if fm.dir != "test" {
		t.Fatalf("got: %s, expected: test", fm.dir)
	}

	// start off with clean slate
	fm.purge()

	// append first item
	_, err = fm.append([]byte("abc"))
	if err != nil {
		t.Fatal(err)
	}

	// file should be loaded
	if len(fm.loadedFiles) != 1 {
		t.Fatalf("got: %d, expected: 1", len(fm.loadedFiles))
	}
	_, exists := fm.loadedFiles["test/00000000000000000.dat"]
	if exists == false {
		log.Fatal("file key does not exist")
	}

	// check master index
	if fm.masterIndex != 1 {
		t.Fatalf("got: %d, expected: 1", fm.masterIndex)
	}

	// add 1000 more strings. this should rollover to a new file
	for i := 0; i < 1000; i++ {
		fm.append([]byte("abc"))
	}

	if len(fm.loadedFiles) != 2 {
		t.Fatalf("got: %d, expected: 2", len(fm.loadedFiles))
	}
	_, exists = fm.loadedFiles["test/00000000000000001.dat"]
	if exists == false {
		log.Fatal("file key does not exist")
	}
	if fm.masterIndex != 1001 {
		t.Fatalf("got: %d, expected: 1001", fm.masterIndex)
	}

	// load an existing fileManager
	fm2, err := createFileManager("test", 1)
	if err != nil {
		t.Fatalf("error creating fileManager: %v", err)
	}

	// ensure everything was loaded properly
	if fm2.masterIndex != 1001 {
		t.Fatalf("got: %d, expected: 1001", fm2.masterIndex)
	}

	// a read should trigger a file load
	raw, err := fm2.read(0)
	if string(raw) != "abc" {
		t.Fatalf("got: %s, expected: abc", string(raw))
	}
	if len(fm2.loadedFiles) != 1 {
		t.Fatalf("got: %d, expected: 1", len(fm2.loadedFiles))
	}
	_, exists = fm2.loadedFiles["test/00000000000000000.dat"]
	if exists == false {
		log.Fatal("file key does not exist")
	}

	// clean-up
	fm.purge()
}
