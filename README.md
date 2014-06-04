GoInMQ
======

An in-process persistent message queue package for Golang.


Example usage:
```
func main() {
	q := goinmq.NewQueue(errLog) // Can pass nil if no logging desired.
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
		fmt.Println("Recv:", msg)
	}
}

func sendMessages(sendChan chan *goinmq.Message) {
	for i := 1; ; i++ {
		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		msg := goinmq.NewMessage()
		msg.Message = fmt.Sprintf("This is message # %v", i)
		fmt.Println("Send:", msg)
		sendChan <- msg
	}
}
```
