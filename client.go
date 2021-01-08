package wal

// Client is the main interface to the WAL
type Client struct {
	Directory string
	db        *db
}

// CreateClient creates a new Client. maxLoadedFiles is the maximum number of pages to load into memory
// dir is the directory where all the data files will be stored
func CreateClient(dir string, maxLoadedFiles uint8) (*Client, error) {
	db, err := createDB(dir, maxLoadedFiles)
	if err != nil {
		return nil, err
	}

	var c Client
	c.Directory = dir
	c.db = db
	return &c, nil
}

// Files returns a list of all the files that the client is managing
func (c *Client) Files() ([]string, error) {
	return c.db.storedFiles()
}

// Write a new entry to the file
func (c *Client) Write(file string, dat []byte) (uint64, error) {
	fm, exists := c.db.files[file]
	if exists {
		return fm.append(dat)
	}

	c.db.createFile(file)
	return c.db.files[file].append(dat)
}

// Read a single entry from the file
func (c *Client) Read(file string, offset uint64) (Message, error) {
	var m Message

	// ensure file exists
	c.db.createFile(file)

	raw, err := c.db.files[file].read(offset)
	if err != nil {
		return m, err
	}

	m.Data = raw
	m.Offset = offset

	return m, nil
}

// ReadFrom reads all entries greater then offset
func (c *Client) ReadFrom(file string, offset uint64) ([]Message, error) {
	var ret []Message

	// ensure file exists
	c.db.createFile(file)

	i := offset
	for {
		raw, err := c.db.files[file].read(i)
		if err != nil {
			if err.Error() == "invalid index" {
				break
			} else {
				return ret, err
			}
		}

		var m Message
		m.Data = raw
		m.Offset = i
		ret = append(ret, m)

		i++
	}

	return ret, nil
}
