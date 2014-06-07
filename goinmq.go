package goinmq

import (
	"time"
)

type Message struct {
	Id         string `json:"id"`
	Priority   int    `json:"priority"`
	CreatedAt  string `json:"created-at"`
	DeadlineAt string `json:"deadline-at"`
	Message    string `json:"message"`
}

type Queue struct {
	Log   Logger
	store Storer
}

type Storer interface {
	SetName(string)
	Peek() (*Message, bool)
	RemoveHead()
	Enqueue(*Message)
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

const (
	QueueNameDefault = "default.goinmq"
)

var (
	reads  chan *readOp
	writes chan *writeOp
)

func NewQueue(queueName string, store Storer, errLog Logger) *Queue {
	if errLog == nil {
		errLog = ErrorLog{}
	}

	store.SetName(queueName)
	errLog.Info("NewQueue(Logger)")

	reads = make(chan *readOp)
	writes = make(chan *writeOp)

	q := &Queue{
		store: store,
		Log:   errLog,
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
				topMsg, ok := q.store.Peek()
				if ok {
					read.resp <- topMsg
					q.store.RemoveHead()
				} else {
					read.resp <- nil
				}
			case write := <-writes:
				m := write.val
				q.store.Enqueue(m)
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
