package goinmq

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/ugorji/go/codec"
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

	if !store.queueExists() {
		err := os.Mkdir(store.QueueName, 0600)
		if err != nil {
			store.Log.Error(err.Error())
			panic(err)
		}
	}

	return store
}

func (s DirectoryStore) SetName(queueName string) {
	s.QueueName = queueName
}

func (s DirectoryStore) queueExists() bool {
	_, err := os.Stat(s.QueueName)

	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		return false
	}
	return true
}

func (s DirectoryStore) isQueueEmpty() bool {
	stat, err := os.Stat(s.QueueName)
	if err != nil {
		panic(err)
	}

	if stat.Size() == 0 {
		return true
	} else {
		return false
	}
}

func (s DirectoryStore) Enqueue(newMsg *Message) {
	s.persist(newMsg)
}

func (s DirectoryStore) persist(msg *Message) {
	lastFilename, fileCount := s.getLastFilename()
	if fileCount > 0 {
		filenum, err := strconv.Atoi(lastFilename)
		if err != nil {
			panic(err)
		}
		if filenum >= 2000000000 {
			s.reindexFiles()
			lastFilename, fileCount = s.getLastFilename()
		}
	} else {
		lastFilename = "0"
	}
	filenum, err := strconv.Atoi(lastFilename)
	if err != nil {
		panic(err)
	}
	filenum++
	formattedFilename := s.formatFilename(filenum)
	filePath := path.Join(s.QueueName, formattedFilename)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//s.marshalGob(file, msg)
	//s.marshalJson(file, msg)
	s.marshalMsgpack(file, msg)
}

func (s DirectoryStore) marshalGob(file *os.File, message *Message) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(message)
	if err != nil {
		panic(err)
	}

	if _, err := file.Write(b.Bytes()); err != nil {
		panic(err)
	}
}

func (s DirectoryStore) marshalJson(file *os.File, message *Message) {
	js, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}

	if _, err := file.Write(js); err != nil {
		panic(err)
	}
}

func (s DirectoryStore) marshalMsgpack(file *os.File, message *Message) {
	var mh codec.MsgpackHandle
	var b []byte
	enc := codec.NewEncoderBytes(&b, &mh)
	err := enc.Encode(message)
	if err != nil {
		panic(err)
	}

	if _, err := file.Write(b); err != nil {
		panic(err)
	}
}

func (s DirectoryStore) getLastFilename() (string, int) {
	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
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
	files, err := ioutil.ReadDir(s.QueueName)
	if err != nil {
		return nil, "", false
	}

	if len(files) == 0 {
		return nil, "", false
	}

	//msg := s.unmarshalGob(files[0])
	//msg := s.unmarshalJson(files[0])
	msg := s.unmarshalMsgpack(files[0])

	return msg, files[0].Name(), true
}

func (s DirectoryStore) unmarshalGob(fileInfo os.FileInfo) *Message {
	file, err := os.Open(path.Join(s.QueueName, fileInfo.Name()))
	if err != nil {
		return nil
	}
	defer file.Close()

	msg := NewMessage()
	d := gob.NewDecoder(file)
	err = d.Decode(&msg)
	if err != nil {
		panic(err)
	}

	return msg
}

func (s DirectoryStore) unmarshalJson(fileInfo os.FileInfo) *Message {
	msgBytes, err := ioutil.ReadFile(path.Join(s.QueueName, fileInfo.Name()))
	if err != nil {
		return nil
	}

	msg := NewMessage()
	err = json.Unmarshal(msgBytes, &msg)
	if err != nil {
		panic(err)
	}

	return msg
}

func (s DirectoryStore) unmarshalMsgpack(fileInfo os.FileInfo) *Message {
	msgBytes, err := ioutil.ReadFile(path.Join(s.QueueName, fileInfo.Name()))
	if err != nil {
		return nil
	}
	msg := NewMessage()
	var mh codec.MsgpackHandle
	dec := codec.NewDecoderBytes(msgBytes, &mh)
	err = dec.Decode(&msg)
	if err != nil {
		panic(err)
	}

	return msg
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
