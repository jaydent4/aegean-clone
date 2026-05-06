package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	raft "go.etcd.io/raft/v3"
)

const enableLoggingEnv = "AEGEAN_ENABLE_LOGGING"

type discardRaftLogger struct{}

func (discardRaftLogger) Debug(v ...interface{})                 {}
func (discardRaftLogger) Debugf(format string, v ...interface{}) {}
func (discardRaftLogger) Error(v ...interface{})                 {}
func (discardRaftLogger) Errorf(format string, v ...interface{}) {}
func (discardRaftLogger) Info(v ...interface{})                  {}
func (discardRaftLogger) Infof(format string, v ...interface{})  {}
func (discardRaftLogger) Warning(v ...interface{})               {}
func (discardRaftLogger) Warningf(format string, v ...interface{}) {
}
func (discardRaftLogger) Fatal(v ...interface{}) {
	os.Exit(1)
}
func (discardRaftLogger) Fatalf(format string, v ...interface{}) {
	os.Exit(1)
}
func (discardRaftLogger) Panic(v ...interface{}) {
	panic(fmt.Sprint(v...))
}
func (discardRaftLogger) Panicf(format string, v ...interface{}) {
	panic(fmt.Sprintf(format, v...))
}

func configureLoggingFromEnv() {
	if loggingEnabledFromEnv() {
		log.SetOutput(os.Stderr)
		raftLogger := &raft.DefaultLogger{Logger: log.New(os.Stderr, "raft ", log.LstdFlags)}
		raft.SetLogger(raftLogger)
		return
	}

	log.SetOutput(io.Discard)
	raft.SetLogger(discardRaftLogger{})
}

func loggingEnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(enableLoggingEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
