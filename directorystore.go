package goinmq

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

type DirectoryStore struct {
	QueueName string
	Log       Logger
}

func NewDirectoryStore(errLog Logger) *DirectoryStore {
	if errLog == nil {
		errLog = ErrorLog{}
	}

	store := &DirectoryStore{}
	store.Log = errLog
	store.QueueName = QueueNameDefault

	return store
}

func (s DirectoryStore) SetName(queueName string) {
	s.QueueName = queueName
}

func (s DirectoryStore) queueExists() bool {
	_, err := os.Stat(s.QueueName)
	if os.IsNotExist(err) {
		s.Log.Error("queueExists() - false, not exists")
		return false
	}
	if err != nil {
		s.Log.Error("queueExists() - false, stat failed")
		return false
	}
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
	s.persist(newMsg)
}

func (s DirectoryStore) persist(msg *Message) {
	if !s.queueExists() {
		err := os.Mkdir(s.QueueName, 0600)
		if err != nil {
			s.Log.Error(err.Error())
			panic(err)
		}
	}

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
	if !s.queueExists() {
		return nil, "", false
	}

	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
		s.Log.Error(err.Error())
		return nil, "", false
	}

	if len(files) == 0 {
		return nil, "", false
	}

	msgBytes, err := ioutil.ReadFile(path.Join(s.QueueName, files[0].Name()))
	if err != nil {
		s.Log.Error(err.Error())
		return nil, "", false
	}

	msg := NewMessage()
	json.Unmarshal(msgBytes, &msg)

	return msg, files[0].Name(), true
}

func (s DirectoryStore) RemoveHead() {
	_, filename, ok := s.getHead()
	if !ok {
		return
	}
	os.Remove(path.Join(s.QueueName, filename))
}

func (s DirectoryStore) ReadQueue() string {
	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
		panic(err)
	}

	dumpFile, err := os.OpenFile("dump."+s.QueueName+".txt", os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer dumpFile.Close()

	os.Chdir(s.QueueName)
	for _, f := range files {
		msgFileBytes, err := ioutil.ReadFile(f.Name())
		if err != nil {
			panic(err)
		}
		dumpFile.Write(msgFileBytes)
	}
	os.Chdir("..")

	// TODO: decide.
	return ""
}
