package spnr

import "log"

type logger interface {
	Printf(format string, v ...interface{})
}

type defaultLogger struct{}

func newDefaultLogger() *defaultLogger {
	return &defaultLogger{}
}

func (d *defaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}
