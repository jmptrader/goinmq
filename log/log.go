package log

import (
	"io"
	"log"
	"os"
)

type Log struct {
	trace          *log.Logger
	info           *log.Logger
	warning        *log.Logger
	error          *log.Logger
	TraceEnabled   bool
	InfoEnabled    bool
	WarningEnabled bool
	ErrorEnabled   bool
}

func NewLog() *Log {
	return &Log{}
}

func (l *Log) GetLogFile(logFilename string) io.Writer {
	logFile, err := os.OpenFile(logFilename+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
		panic(err)
	}
	return logFile
}

func (l *Log) InitLog(traceHandle io.Writer, infoHandle io.Writer, warningHandle io.Writer, errorHandle io.Writer) {
	l.trace = log.New(traceHandle,
		"TRACE: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	l.info = log.New(infoHandle,
		"INFO: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	l.warning = log.New(warningHandle,
		"WARNING: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	l.error = log.New(errorHandle,
		"ERROR: ",
		log.Ldate|log.Ltime|log.Lshortfile)
}

func (l *Log) Trace(message string) {
	if l.TraceEnabled {
		l.trace.Println(message)
	}
}

func (l *Log) Info(message string) {
	if l.InfoEnabled {
		l.info.Println(message)
	}
}

func (l *Log) Warning(message string) {
	if l.WarningEnabled {
		l.warning.Println(message)
	}
}

func (l *Log) Error(message string) {
	if l.ErrorEnabled {
		l.error.Println(message)
	}
}
