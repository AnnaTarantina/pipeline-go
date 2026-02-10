package logger

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Logger struct {
	prefix string
}

func New(prefix string) *Logger {
	return &Logger{prefix: prefix}
}

func (l *Logger) Info(msg string, args ...interface{}) {
	l.log("INFO", msg, args...)
}

func (l *Logger) Debug(msg string, args ...interface{}) {
	l.log("DEBUG", msg, args...)
}

func (l *Logger) Warn(msg string, args ...interface{}) {
	l.log("WARN", msg, args...)
}

func (l *Logger) Error(msg string, args ...interface{}) {
	l.log("ERROR", msg, args...)
}

func (l *Logger) Stage(stageName string, msg string, args ...interface{}) {
	fullMsg := fmt.Sprintf("[%s] %s", stageName, msg)
	l.log("STAGE", fullMsg, args...)
}

func (l *Logger) log(level, msg string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	prefix := ""
	if l.prefix != "" {
		prefix = fmt.Sprintf("[%s] ", l.prefix)
	}

	formattedMsg := msg
	if len(args) > 0 {
		formattedMsg = fmt.Sprintf(msg, args...)
	}

	log.SetOutput(os.Stdout)
	log.SetFlags(0)
	log.Printf("%s | %-5s | %s%s", timestamp, level, prefix, formattedMsg)
}
