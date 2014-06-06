package main

import (
	"fmt"
	"github.com/russmack/goinmq"
	"github.com/russmack/goinmq/log"
	"math/rand"
	"time"
)

var errLog *log.Log

func init() {
	rand.Seed(time.Now().UTC().UnixNano())

	errLog = log.NewLog()
	logFile := errLog.GetLogFile("errlog")
	errLog.InitLog(logFile, logFile, logFile, logFile)
	errLog.TraceEnabled = false
	errLog.InfoEnabled = true
	errLog.WarningEnabled = true
	errLog.ErrorEnabled = true
	errLog.Info("Starting client.")
}

func main() {
	store := goinmq.NewFileStore(errLog)

	q := goinmq.NewQueue("", store, errLog) // Can pass nil if no logging desired.
	recvChan := q.GetReceiveChannel()
	sendChan := q.GetSendChannel()
	go recvMessages(recvChan)
	go sendMessages(sendChan)

	fin := make(chan bool)
	for {
		<-fin
	}
}

func recvMessages(recvChan chan *goinmq.Message) {
	for {
		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		msg := <-recvChan
		sent, _ := time.Parse(time.RFC3339, msg.CreatedAt)
		fmt.Println("Client Recv:", msg, "was sent:", msg.CreatedAt, " :: ", time.Now().Sub(sent), "ago")
	}
}

func sendMessages(sendChan chan *goinmq.Message) {
	for i := 1; ; i++ {
		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		msg := goinmq.NewMessage()
		msg.Message = fmt.Sprintf("This is message # %v", i)
		fmt.Println("Client Send:", msg)
		sendChan <- msg
	}
}
