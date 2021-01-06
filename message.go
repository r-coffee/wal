package wal

// Message is the object that is written to the file
type Message struct {
	Data   []byte
	Offset uint64
}
