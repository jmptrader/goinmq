# GoInMQ

An in-process persistent message queue package for Golang.

![Progress](http://progressed.io/bar/80?title=ok)

---
#### Status: Good, not optimised.
---

## Usage
```
func main() {
	// Empty queue name defaults, nil logger if no logging desired.
	q := goinmq.NewQueue("", errLog)
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

## Checklist

 - [X] One file per message.
 - [X] One file for all messages.
 - [X] Json.
 - [X] Msgpack.
 - [X] Gob.
 - [X] Provide examples.
 - [ ] Write API documentation.
 - [ ] Write tests.

## License
BSD 3-Clause: [LICENSE.txt](LICENSE.txt)

[<img alt="LICENSE" src="http://img.shields.io/pypi/l/Django.svg?style=flat-square"/>](LICENSE.txt)
