// GoInMq is an in-process persistent message queue.

// Store types and store formats are interchangeable.
package goinmq

import (
	"time"
)

// Message is a queue message.
type Message struct {
	Id         string `json:"id"`
	Priority   int    `json:"priority"`
	CreatedAt  string `json:"created-at"`
	DeadlineAt string `json:"deadline-at"`
	Message    string `json:"message"`
}

// Queue is the message queue.
type Queue struct {
	Log   Logger
	store Storer
}

// Storer is implemented by all store types.
type Storer interface {
	SetName(string)
	Peek() (*Message, bool)
	RemoveHead()
	Enqueue(*Message)
}

// Logger is implemented by all logging types.
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

// ReadOp contains the channel into which to send the read message.
type readOp struct {
	resp chan *Message
}

// WriteOp contains the message to be enqueued
// and the channel on which to send the ack response.
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

// NewQueue returns a new queue with specified name and store.
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

// NewMessage returns a new Message.
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

// GetSendChannel returns the channel for enqueuing.
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

// GetReceiveChannel returns the channel for dequeuing.
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
