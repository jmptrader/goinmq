package goinmq

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"time"
)

const (
	FilenameDefault = "default.goinmq"
	FileExtQueue    = "wal"
	FileExtTmp      = "tmp"
)

type Message struct {
	Id         string `json:"id"`
	Priority   int    `json:"priority"`
	CreatedAt  string `json:"created-at"`
	DeadlineAt string `json:"deadline-at"`
	Message    string `json:"message"`
}

type Queue struct {
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

type readOp struct {
	resp chan *Message
}

type writeOp struct {
	val  *Message
	resp chan bool
}

var (
	reads  chan *readOp
	writes chan *writeOp
)

func NewQueue(queueFilename string, errLog Logger) *Queue {
	if errLog == nil {
		errLog = ErrorLog{}
	}
	if queueFilename == "" {
		queueFilename = FilenameDefault
	}

	errLog.Info("NewQueue(Logger)")

	reads = make(chan *readOp)
	writes = make(chan *writeOp)

	q := &Queue{
		queueFilename:    queueFilename + "." + FileExtQueue,
		tmpQueueFilename: queueFilename + "." + FileExtTmp,
		Log:              errLog,
	}
	q.startFsGate(reads, writes)
	return q
}

func NewMessage() *Message {
	return &Message{}
}

func (q Queue) startFsGate(reads chan *readOp, writes chan *writeOp) {
	go func() {
		for {
			select {
			case read := <-reads:
				topMsg, _, ok := q.peek()
				if ok {
					q.Log.Trace("lib read " + topMsg.Message)
					read.resp <- topMsg
					q.removeHead()
				} else {
					read.resp <- nil
				}
			case write := <-writes:
				m := write.val
				q.Log.Trace("lib write " + m.Message)
				q.enqueue(m)
				write.resp <- true
			}
		}
	}()
}

func (q Queue) GetSendChannel() chan *Message {
	q.Log.Trace("GetSendQueue()")

	sendChan := make(chan *Message)

	go func() {
		for {
			newMsg := <-sendChan
			newMsg.CreatedAt = time.Now().UTC().Format(time.RFC3339)
			write := &writeOp{
				val:  newMsg,
				resp: make(chan bool)}
			writes <- write
			<-write.resp
		}
	}()
	return sendChan
}

func (q Queue) GetReceiveChannel() chan *Message {
	q.Log.Trace("GetReceiveQueue()")

	recvChan := make(chan *Message)

	go func() {
		for {
			q.Log.Trace("Checking for messages to dequeue")
			read := &readOp{
				resp: make(chan *Message)}
			reads <- read
			topMsg := <-read.resp
			if topMsg != nil {
				recvChan <- topMsg
			}
		}
	}()
	return recvChan
}

func (q Queue) enqueue(newMsg *Message) {
	q.Log.Trace("enqueue(Message)")

	q.persist(newMsg)
}

func (q Queue) queueExists() bool {
	q.Log.Trace("queueExists()")

	fileInfo, err := os.Stat(q.queueFilename)
	q.Log.Trace("queueExists() - called stat")
	if err != nil {
		q.Log.Trace("queueExists() - false, stat failed")
		return false
	}
	q.Log.Trace("queueExists() - stat checked")
	walSize := fileInfo.Size()
	q.Log.Trace("queueExists() - size retrieved")
	if walSize == 0 {
		q.Log.Trace("queueExists() - false, size 0")
		return false
	}

	q.Log.Trace("queueExists() - true")
	return true
}

func (q Queue) persist(msg *Message) {
	q.Log.Trace("persist(Message)")

	q.Log.Trace("persist(Message) is creating queue file")
	file, err := os.OpenFile(q.queueFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	defer file.Close()

	js, _ := json.Marshal(msg)

	if _, err := file.WriteString(string(js) + "\n"); err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
}

func (q Queue) peek() (*Message, int64, bool) {
	q.Log.Trace("peek()")

	if !q.queueExists() {
		q.Log.Trace("no queue")
		return nil, 0, false
	}
	q.Log.Trace("found queue")

	file, err := os.Open(q.queueFilename)
	if err != nil {
		q.Log.Error(err.Error())
		return nil, 0, false
	}
	defer file.Close()

	q.Log.Trace("reading head")

	reader := bufio.NewReader(file)
	msgData, err := reader.ReadString('\n')
	if err != nil {
		q.Log.Error(err.Error())
		return nil, 0, false
	}
	q.Log.Trace("got head")

	msg := NewMessage()
	msgBytes := []byte(msgData)
	json.Unmarshal(msgBytes, &msg)

	return msg, int64(len(msgData)), true
}

func (q Queue) removeHead() {
	q.Log.Trace("deleteTopMessage(offset)")

	_, size, ok := q.peek()
	if !ok {
		return
	}
	q.createLogTail(size + 1) // 1 is "\n"

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
		q.Log.Trace("swapTmp() remove queue failed")
		q.Log.Error(err.Error())
		panic(err)
	}
	q.Log.Trace("swapTmp() remove queue done")

	q.Log.Trace("swapTmp() renaming")
	err := os.Rename(q.tmpQueueFilename, q.queueFilename)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
}

func (q Queue) ReadQueue() string {
	text, err := ioutil.ReadFile(q.queueFilename)
	if err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
	return string(text)
}
