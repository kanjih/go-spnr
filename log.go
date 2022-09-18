package spnr

import "log"

type logger interface {
	Printf(format string, v ...any)
}

type defaultLogger struct{}

func newDefaultLogger() *defaultLogger {
	return &defaultLogger{}
}

func (d *defaultLogger) Printf(format string, v ...any) {
	log.Printf(format, v...)
}
