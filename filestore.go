package goinmq

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
)

const (
	QueueNameDefault = "default.goinmq"
	FileExtQueue     = "wal"
	FileExtTmp       = "tmp"
)

type FileStore struct {
	QueueName        string
	queueFilename    string
	tmpQueueFilename string
	Log              Logger
}

func NewFileStore(errLog Logger) *FileStore {
	if errLog == nil {
		errLog = ErrorLog{}
	}

	store := &FileStore{}
	store.Log = errLog
	store.QueueName = QueueNameDefault
	store.queueFilename = QueueNameDefault + "." + FileExtQueue
	store.tmpQueueFilename = QueueNameDefault + "." + FileExtTmp

	return store
}

func (s FileStore) SetName(queueName string) {
	s.QueueName = queueName
	s.queueFilename = queueName + "." + FileExtQueue
	s.tmpQueueFilename = queueName + "." + FileExtTmp
}

func (s FileStore) queueExists() bool {
	s.Log.Trace("queueExists()")

	fileInfo, err := os.Stat(s.queueFilename)
	s.Log.Trace("queueExists() - called stat")
	if err != nil {
		s.Log.Trace("queueExists() - false, stat failed")
		return false
	}
	s.Log.Trace("queueExists() - stat checked")
	walSize := fileInfo.Size()
	s.Log.Trace("queueExists() - size retrieved")
	if walSize == 0 {
		s.Log.Trace("queueExists() - false, size 0")
		return false
	}

	s.Log.Trace("queueExists() - true")
	return true
}

func (s FileStore) Enqueue(newMsg *Message) {
	s.Log.Trace("enqueue(Message)")

	s.persist(newMsg)
}

func (s FileStore) persist(msg *Message) {
	s.Log.Trace("persist(Message)")

	s.Log.Trace("persist(Message) is creating queue file")
	file, err := os.OpenFile(s.queueFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	defer file.Close()

	js, _ := json.Marshal(msg)

	if _, err := file.WriteString(string(js) + "\n"); err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
}

func (s FileStore) Peek() (*Message, int64, bool) {
	s.Log.Trace("peek()")

	if !s.queueExists() {
		s.Log.Trace("no queue")
		return nil, 0, false
	}
	s.Log.Trace("found queue")

	file, err := os.Open(s.queueFilename)
	if err != nil {
		s.Log.Error(err.Error())
		return nil, 0, false
	}
	defer file.Close()

	s.Log.Trace("reading head")

	//reader := io.Re
	reader := bufio.NewReader(file)
	msgData, err := reader.ReadString('\n')
	if err != nil {
		s.Log.Error(err.Error())
		return nil, 0, false
	}
	s.Log.Trace("got head")

	msg := NewMessage()
	msgBytes := []byte(msgData)
	json.Unmarshal(msgBytes, &msg)

	return msg, int64(len(msgData)), true
}

func (s FileStore) RemoveHead() {
	s.Log.Trace("deleteTopMessage(offset)")

	_, size, ok := s.Peek()
	if !ok {
		return
	}
	s.createLogTail(size + 1) // 1 is "\n"

	s.swapTmp()
}

func (s FileStore) createLogTail(offset int64) {
	s.Log.Trace("createLogTail(offset)")

	tmpFile := s.getTmpFile()
	defer tmpFile.Close()

	queueFile := s.getQueueFile()
	defer queueFile.Close()

	buff := make([]byte, 1024)
	_, err := queueFile.Seek(offset, 0)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	for {
		nRead, err := queueFile.Read(buff)
		if err != nil && err != io.EOF {
			s.Log.Error(err.Error())
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
	s.Log.Trace("getQueueFile()")

	file, err := os.OpenFile(s.queueFilename, os.O_CREATE, 0600)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	return file
}

func (s FileStore) getTmpFile() *os.File {
	s.Log.Trace("getTmpFile()")

	file, err := os.OpenFile(s.tmpQueueFilename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	return file
}

func (s FileStore) swapTmp() {
	s.Log.Trace("swapTmp()")

	if err := os.Remove(s.queueFilename); err != nil {
		s.Log.Trace("swapTmp() remove queue failed")
		s.Log.Error(err.Error())
		panic(err)
	}
	s.Log.Trace("swapTmp() remove queue done")

	s.Log.Trace("swapTmp() renaming")
	err := os.Rename(s.tmpQueueFilename, s.queueFilename)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
}

func (s FileStore) ReadQueue() string {
	text, err := ioutil.ReadFile(s.queueFilename)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	return string(text)
}
