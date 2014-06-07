package goinmq

import (
	//"bufio"
	"encoding/json"
	"fmt"
	//"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

const ()

type DirectoryStore struct {
	QueueName string
	//queueFilename    string
	//tmpQueueFilename string
	Log Logger
}

func NewDirectoryStore(errLog Logger) *DirectoryStore {
	if errLog == nil {
		errLog = ErrorLog{}
	}

	store := &DirectoryStore{}
	store.Log = errLog
	store.QueueName = QueueNameDefault
	//store.queueFilename = QueueNameDefault + "." + FileExtQueue
	//store.tmpQueueFilename = QueueNameDefault + "." + FileExtTmp

	return store
}

func (s DirectoryStore) SetName(queueName string) {
	s.QueueName = queueName
	//s.queueFilename = queueName + "." + FileExtQueue
	//s.tmpQueueFilename = queueName + "." + FileExtTmp
}

func (s DirectoryStore) queueExists() bool {
	s.Log.Trace("queueExists()")

	_, err := os.Stat(s.QueueName)
	s.Log.Trace("queueExists() - called stat")
	if os.IsNotExist(err) {
		s.Log.Trace("queueExists() - false, not exists")
		return false
	}
	if err != nil {
		s.Log.Trace("queueExists() - false, stat failed")
		return false
	}
	s.Log.Trace("queueExists() - stat checked")
	s.Log.Trace("queueExists() - true")
	return true
}

func (s DirectoryStore) isQueueEmpty() bool {
	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	if len(files) != 0 {
		return false
	} else {
		return true
	}
}

func (s DirectoryStore) Enqueue(newMsg *Message) {
	s.Log.Trace("enqueue(Message)")

	s.persist(newMsg)
}

func (s DirectoryStore) persist(msg *Message) {
	s.Log.Trace("persist(Message)")

	if !s.queueExists() {
		err := os.Mkdir(s.QueueName, 0600)
		if err != nil {
			s.Log.Error(err.Error())
			panic(err)
		}
	}

	s.Log.Trace("persist(Message) is creating queue file")
	lastFilename, fileCount := s.getLastFilename()
	if fileCount > 0 {
		filenum, err := strconv.Atoi(lastFilename)
		if err != nil {
			s.Log.Error(err.Error())
			panic(err)
		}
		if filenum >= 2000000000 {
			s.reindexFiles()
			lastFilename, fileCount = s.getLastFilename()
		}
	}

	if fileCount == 0 {
		lastFilename = "0"
	}
	filenum, err := strconv.Atoi(lastFilename)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	filenum++
	formattedFilename := s.formatFilename(filenum)
	filePath := path.Join(s.QueueName, formattedFilename)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	defer file.Close()

	js, _ := json.Marshal(msg)

	if _, err := file.WriteString(string(js)); err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
}

func (s DirectoryStore) getLastFilename() (string, int) {
	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	fileCount := len(files)
	lastFilename := ""
	if fileCount > 0 {
		lastFilename = files[len(files)-1].Name()
	}
	return lastFilename, fileCount
}

func (s DirectoryStore) formatFilename(filename int) string {
	formatted := fmt.Sprintf("%010d", filename)
	return formatted
}

func (s DirectoryStore) reindexFiles() {
	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	os.Chdir(s.QueueName)
	for i, j := range files {
		os.Rename(j.Name(), s.formatFilename(i))
	}
	os.Chdir("..")
}

func (s DirectoryStore) Peek() (*Message, bool) {
	msg, _, found := s.getHead()
	return msg, found
}

func (s DirectoryStore) getHead() (*Message, string, bool) {
	s.Log.Trace("gethead()")

	if !s.queueExists() {
		s.Log.Trace("no queue")
		return nil, "", false
	}
	s.Log.Trace("found queue")

	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
		s.Log.Error(err.Error())
		return nil, "", false
	}

	if len(files) == 0 {
		return nil, "", false
	}

	s.Log.Trace("reading head")

	msgBytes, err := ioutil.ReadFile(path.Join(s.QueueName, files[0].Name()))
	if err != nil {
		s.Log.Error(err.Error())
		return nil, "", false
	}
	s.Log.Trace("got head")

	msg := NewMessage()
	//msgBytes := []byte(msgData)
	json.Unmarshal(msgBytes, &msg)

	return msg, files[0].Name(), true
}

func (s DirectoryStore) RemoveHead() {
	s.Log.Trace("deleteTopMessage(offset)")

	_, filename, ok := s.getHead()
	if !ok {
		return
	}
	//s.createLogTail(size + 1) // 1 is "\n"
	//s.swapTmp()
	os.Remove(path.Join(s.QueueName, filename))
}

/* TODO: implement read all files and join strings.
func (s DirectoryStore) ReadQueue() string {
	text, err := ioutil.ReadFile(s.queueFilename)
	if err != nil {
		s.Log.Error(err.Error())
		panic(err)
	}
	return string(text)
}
*/