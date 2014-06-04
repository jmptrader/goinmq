package goinmq

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

const (
	Filename     = "persistedqueue"
	FileExtQueue = "wal"
	FileExtTmp   = "tmp"
)

type Message struct {
	Message string
}

type Queue struct {
	sync.Mutex
	Log              Logger
	queueFilename    string
	tmpQueueFilename string
}

type Logger interface {
	Trace(string)
	Info(string)
	Warning(string)
	Error(string)
}

type ErrorLog struct{}

func (ErrorLog) Trace(string)   {}
func (ErrorLog) Info(string)    {}
func (ErrorLog) Warning(string) {}
func (ErrorLog) Error(string)   {}

func NewQueue(errLog Logger) *Queue {
	if errLog == nil {
		errLog = ErrorLog{}
	}
	errLog.Info("NewQueue(Logger)")

	return &Queue{
		queueFilename:    Filename + "." + FileExtQueue,
		tmpQueueFilename: Filename + "." + FileExtTmp,
		Log:              errLog,
	}
}

func NewMessage() *Message {
	return &Message{}
}

func (q Queue) ReadQueue() string {
	text, err := ioutil.ReadFile(q.queueFilename)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	return string(text)
}

func (q Queue) GetSendChannel() chan *Message {
	q.Log.Trace("GetSendQueue()")

	sendChan := make(chan *Message)

	go func() {
		for {
			newMsg := <-sendChan
			q.enqueue(newMsg)
		}
	}()
	return sendChan
}

func (q Queue) GetReceiveChannel() chan *Message {
	q.Log.Trace("GetReceiveQueue()")

	recvChan := make(chan *Message)

	go func() {
		for {
			topMsg, ok := q.dequeue()
			if ok == false {
				continue
			}
			recvChan <- topMsg
		}
	}()
	return recvChan
}

func (q Queue) enqueue(newMsg *Message) {
	q.Log.Trace("enqueue(Message)")

	q.persist(newMsg)
}

func (q Queue) dequeue() (*Message, bool) {
	q.Log.Trace("dequeue()")

	q.Lock()

	fileInfo, err := os.Stat(q.queueFilename)
	if err != nil {
		return NewMessage(), false
	}
	walSize := fileInfo.Size()
	if walSize == 0 {
		return NewMessage(), false
	}
	msg := q.popQueue()
	q.Unlock()

	return msg, true
}

func (q Queue) persist(msg *Message) {
	q.Log.Trace("persist(Message)")

	q.Lock()

	file, err := os.OpenFile(q.queueFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	defer file.Close()

	if _, err := file.WriteString(msg.Message + "\n"); err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	q.Unlock()
}

func (q Queue) popQueue() *Message {
	q.Log.Trace("popQueue()")

	msgData := q.readNext()
	q.deleteTopMessage(int64(len(msgData.Message + "\n")))

	return msgData
}

func (q Queue) readNext() *Message {
	q.Log.Trace("readNext()")

	file, err := os.Open(q.queueFilename)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	scanner := bufio.NewScanner(reader)
	scanner.Scan()
	msgData := scanner.Text()
	if err := scanner.Err(); err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}

	msg := NewMessage()
	msg.Message = msgData

	return msg
}

func (q Queue) deleteTopMessage(offset int64) {
	q.Log.Trace("deleteTopMessage(offset)")

	q.createLogTail(offset)
	q.swapTmp()
}

func (q Queue) createLogTail(offset int64) {
	q.Log.Trace("createLogTail(offset)")

	tmpFile := q.getTmpFile()
	defer tmpFile.Close()

	queueFile := q.getQueueFile()
	defer queueFile.Close()

	buff := make([]byte, 1024)
	_, err := queueFile.Seek(offset, 0)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	for {
		nRead, err := queueFile.Read(buff)
		if err != nil && err != io.EOF {
			q.Log.Error(err.Error())
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

func (q Queue) getQueueFile() *os.File {
	q.Log.Trace("getQueueFile()")

	file, err := os.OpenFile(q.queueFilename, os.O_CREATE, 0600)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	return file
}

func (q Queue) getTmpFile() *os.File {
	q.Log.Trace("getTmpFile()")

	file, err := os.OpenFile(q.tmpQueueFilename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	return file
}

func (q Queue) swapTmp() {
	q.Log.Trace("swapTmp()")

	if err := os.Remove(q.queueFilename); err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}

	err := os.Rename(q.tmpQueueFilename, q.queueFilename)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
}
