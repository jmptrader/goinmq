package goinmq

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
)

const (
	FileExtQueue = "wal"
	FileExtTmp   = "tmp"
)

// FileStore stores all messages in a single queue file.
type FileStore struct {
	QueueName        string
	queueFilename    string
	tmpQueueFilename string
	Log              Logger
}

// NewFileStore returns a new FileStore.
func NewFileStore(errLog Logger) *FileStore {
	if errLog == nil {
		errLog = ErrorLog{}
	}

	store := &FileStore{}
	store.Log = errLog
	store.QueueName = QueueNameDefault
	store.queueFilename = QueueNameDefault + "." + FileExtQueue
	store.tmpQueueFilename = QueueNameDefault + "." + FileExtTmp

	if !store.queueExists() {
		err := os.Mkdir(store.QueueName, 0600)
		if err != nil {
			store.Log.Error(err.Error())
			panic(err)
		}
	}

	return store
}

// SetName sets the name of the queue file.
func (s FileStore) SetName(queueName string) {
	s.QueueName = queueName
	s.queueFilename = queueName + "." + FileExtQueue
	s.tmpQueueFilename = queueName + "." + FileExtTmp
}

func (s FileStore) queueExists() bool {
	fileInfo, err := os.Stat(s.queueFilename)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		return false
	}
	walSize := fileInfo.Size()
	if walSize == 0 {
		return false
	}

	return true
}

// Enqueue enqueues a message.
func (s FileStore) Enqueue(newMsg *Message) {
	s.persist(newMsg)
}

func (s FileStore) persist(msg *Message) {
	file, err := os.OpenFile(s.queueFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	js, _ := json.Marshal(msg)

	if _, err := file.WriteString(string(js) + "\n"); err != nil {
		panic(err)
	}
}

// Peek returns the first message in the queue, and whether there was one,
// without removing it from the queue.
func (s FileStore) Peek() (*Message, bool) {
	msg, _, found := s.getHead()
	return msg, found
}

func (s FileStore) getHead() (*Message, int64, bool) {
	file, err := os.Open(s.queueFilename)
	if err != nil {
		return nil, 0, false
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	msgData, err := reader.ReadString('\n')
	if err != nil {
		return nil, 0, false
	}

	msg := NewMessage()
	msgBytes := []byte(msgData)
	json.Unmarshal(msgBytes, &msg)

	return msg, int64(len(msgData)), true
}

// RemoveHead removes the first message in the queue without returning it.
func (s FileStore) RemoveHead() {
	_, size, ok := s.getHead()
	if !ok {
		return
	}
	s.createLogTail(size + 1) // 1 is "\n"
	s.swapTmp()
}

func (s FileStore) createLogTail(offset int64) {
	tmpFile := s.getTmpFile()
	defer tmpFile.Close()

	queueFile := s.getQueueFile()
	defer queueFile.Close()

	buff := make([]byte, 1024)
	_, err := queueFile.Seek(offset, 0)
	if err != nil {
		panic(err)
	}
	for {
		nRead, err := queueFile.Read(buff)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if nRead == 0 {
			break
		}
		if nRead < 1024 {
			tmpFile.Write(buff[:nRead])
		} else {
			tmpFile.Write(buff)
		}
	}
}

func (s FileStore) getQueueFile() *os.File {
	file, err := os.OpenFile(s.queueFilename, os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	return file
}

func (s FileStore) getTmpFile() *os.File {
	file, err := os.OpenFile(s.tmpQueueFilename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	return file
}

func (s FileStore) swapTmp() {
	if err := os.Remove(s.queueFilename); err != nil {
		panic(err)
	}
	err := os.Rename(s.tmpQueueFilename, s.queueFilename)
	if err != nil {
		panic(err)
	}
}

// ReadQueue reads and returns the queue file.
func (s FileStore) ReadQueue() string {
	text, err := ioutil.ReadFile(s.queueFilename)
	if err != nil {
		panic(err)
	}
	return string(text)
}
