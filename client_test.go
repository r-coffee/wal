package wal

import (
	"os"
	"testing"
)

func TestClientRoundTrip(t *testing.T) {
	runningInTest = true

	os.RemoveAll("test")

	c, err := CreateClient("test", 5)
	if err != nil {
		t.Fatalf("not expecting error: %v", err)
	}

	// should start empty
	files, err := c.Files()
	if err != nil {
		t.Fatalf("not expecting error: %v", err)
	}

	if len(files) != 0 {
		t.Fatalf("got: %d expected: 0", len(files))
	}

	// write should create a new file
	offset, err := c.Write("first", []byte("test"))
	if err != nil {
		t.Fatalf("not expecting error: %v", err)
	}

	if offset != 0 {
		t.Fatalf("got: %d expected: 0", offset)
	}

	// write second entry
	offset, err = c.Write("first", []byte("second"))
	if err != nil {
		t.Fatalf("not expecting error: %v", err)
	}

	if offset != 1 {
		t.Fatalf("got: %d expected: 1", offset)
	}

	// we should only have a single file
	files, err = c.Files()
	if err != nil {
		t.Fatalf("not expecting error: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("got: %d expected: 1", len(files))
	}

	// read an entry
	m, err := c.Read("first", 0)
	if err != nil {
		t.Fatalf("not expecting error: %v", err)
	}

	if string(m.Data) != "test" {
		t.Fatalf("got: %s expected: test", string(m.Data))
	}

	// read all entries
	m2, err := c.ReadFrom("first", 0)
	if err != nil {
		t.Fatalf("not expecting error: %v", err)
	}

	if len(m2) != 2 {
		t.Fatalf("got: %d expected: 2", len(m2))
	}

	if string(m2[0].Data) != "test" {
		t.Fatalf("got: %s expected: test", string(m2[0].Data))
	}
	if string(m2[1].Data) != "second" {
		t.Fatalf("got: %s expected: second", string(m2[1].Data))
	}

	if m2[0].Offset != 0 {
		t.Fatalf("got: %d expected: 0", m2[0].Offset)
	}
	if m2[1].Offset != 1 {
		t.Fatalf("got: %d expected: 1", m2[1].Offset)
	}

	os.RemoveAll("test")
}
