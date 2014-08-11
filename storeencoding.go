package goinmq

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"github.com/ugorji/go/codec"
	"io/ioutil"
	"os"
	"path"
)

type StoreEncoding interface {
	Marshal(file *os.File, message *Message)
	Unmarshal(fileInfo os.FileInfo, queueName string) *Message
}

type GobEncoding struct{}
type JsonEncoding struct{}
type MsgpackEncoding struct{}

func (e GobEncoding) Marshal(file *os.File, message *Message) {
	b := new(bytes.Buffer)
	g := gob.NewEncoder(b)
	err := g.Encode(message)
	if err != nil {
		panic(err)
	}
	if _, err := file.Write(b.Bytes()); err != nil {
		panic(err)
	}
}

func (e JsonEncoding) Marshal(file *os.File, message *Message) {
	js, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	if _, err := file.Write(js); err != nil {
		panic(err)
	}
}

func (e MsgpackEncoding) Marshal(file *os.File, message *Message) {
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

func (e GobEncoding) Unmarshal(fileInfo os.FileInfo, queueName string) *Message {
	file, err := os.Open(path.Join(queueName, fileInfo.Name()))
	if err != nil {
		return nil
	}
	defer file.Close()
	msg := NewMessage()
	g := gob.NewDecoder(file)
	err = g.Decode(&msg)
	if err != nil {
		panic(err)
	}
	return msg
}

func (e JsonEncoding) Unmarshal(fileInfo os.FileInfo, queueName string) *Message {
	msgBytes, err := ioutil.ReadFile(path.Join(queueName, fileInfo.Name()))
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

func (e MsgpackEncoding) Unmarshal(fileInfo os.FileInfo, queueName string) *Message {
	msgBytes, err := ioutil.ReadFile(path.Join(queueName, fileInfo.Name()))
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
