package wal

import (
	"os"
	"testing"
)

func TestFileExists(t *testing.T) {
	// positive
	if fileExists("file_test.go") == false {
		t.Fatal("file should exist")
	}

	// negative
	if fileExists("file_does_not_exist") {
		t.Fatal("file should not exist")
	}
}

func TestFileLifecycle(t *testing.T) {
	runningInTest = true

	// clear any existing file
	os.Remove("test.dat")
	os.Remove("test.dat.map")

	// create
	f, err := createOrLoadFile("test.dat")
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	// append 2 rows
	err = f.append(0, []byte("abc"))
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	err = f.append(1, []byte("xyz"))
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	// read data
	raw, err := f.read(0)
	if err != nil {
		t.Fatalf("error reading: %v", err)
	}

	if string(raw) != "abc" {
		t.Fatalf("got: %s, expected: abc", string(raw))
	}

	f.close()

	// reload file and make sure it loads properly
	f, err = createOrLoadFile("test.dat")
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	// check name
	if f.name != "test.dat" {
		t.Fatalf("got: %s, expected: test.dat", f.name)
	}

	// check lookup map
	if len(f.lookup) != 2 {
		t.Fatalf("got: %d, expected: 2", len(f.lookup))
	}

	// read data
	raw, err = f.read(0)
	if err != nil {
		t.Fatalf("error reading: %v", err)
	}

	if string(raw) != "abc" {
		t.Fatalf("got: %s, expected: abc", string(raw))
	}

	raw, err = f.read(1)
	if err != nil {
		t.Fatalf("error reading: %v", err)
	}

	if string(raw) != "xyz" {
		t.Fatalf("got: %s, expected: xyz", string(raw))
	}

	// ensure we can still append
	err = f.append(2, []byte("foo"))
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	raw, err = f.read(2)
	if err != nil {
		t.Fatalf("error reading: %v", err)
	}

	if string(raw) != "foo" {
		t.Fatalf("got: %s, expected: foo", string(raw))
	}

	// perform one final reload
	f.close()
	f, _ = createOrLoadFile("test.dat")
	raw, _ = f.read(2)
	if string(raw) != "foo" {
		t.Fatalf("got: %s, expected: foo", string(raw))
	}

	f.close()
	os.Remove("test.dat")
	os.Remove("test.dat.map")
}
