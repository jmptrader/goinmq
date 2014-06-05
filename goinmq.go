package goinmq

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
)

const (
	FilenameDefault = "default.goinmq"
	FileExtQueue    = "wal"
	FileExtTmp      = "tmp"
)

type Message struct {
	Message string
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
				topMsg, ok := q.peek()
				if ok {
					q.Log.Trace("lib read " + topMsg.Message)
					read.resp <- topMsg
					q.removeHead()
				} else {
					read.resp <- nil
				}
			case write := <-writes:
				m := NewMessage()
				m.Message = "write " + write.val.Message
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
			recvChan <- topMsg
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

	if _, err := file.WriteString(msg.Message + "\n"); err != nil {
		q.Log.Error(err.Error())
		panic(err)
	}
}

func (q Queue) peek() (*Message, bool) {
	q.Log.Trace("peek()")

	if !q.queueExists() {
		q.Log.Trace("no queue")
		return nil, false
	}
	q.Log.Trace("found queue")

	file, err := os.Open(q.queueFilename)
	if err != nil {
		q.Log.Error(err.Error())
		return nil, false
	}
	defer file.Close()

	q.Log.Trace("reading head")

	reader := bufio.NewReader(file)
	scanner := bufio.NewScanner(reader)
	scanner.Scan()
	msgData := scanner.Text()
	if err := scanner.Err(); err != nil {
		q.Log.Error(err.Error())
		return nil, false
	}
	q.Log.Trace("got head")

	msg := NewMessage()
	msg.Message = msgData

	return msg, true
}

func (q Queue) removeHead() {
	q.Log.Trace("deleteTopMessage(offset)")

	// TODO: get rid of this read?
	headMessage, ok := q.peek()
	if !ok {
		return
	}
	offset := int64(len(headMessage.Message + "\n"))
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
