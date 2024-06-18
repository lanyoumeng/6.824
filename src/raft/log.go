package raft

import "fmt"

type Entry struct {
	Term    int
	Command interface{}
}

func (e Entry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", e.Term, e.Command)
}

type Log struct {
	log    []Entry
	index0 int
}

func mkLogEmpty() Log {
	return Log{log: make([]Entry, 1), index0: 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log: log, index0: index0}
}

func (l *Log) append(entry Entry) {
	l.log = append(l.log, entry)

}
func (l *Log) start() int {
	return l.index0
}

func (l *Log) cutend(index int) {
	l.log = l.log[0 : index-l.index0]
}
func (l *Log) cuteStart(index int) {
	l.index0 += index
	l.log = l.log[index:]
}

// 从index开始的日志
func (l *Log) slice(index int) []Entry {
	return l.log[index-l.index0:]
}
func (l *Log) lastindex() int {
	return l.index0 + len(l.log) - 1
}
func (l *Log) entry(index int) *Entry {
	return &(l.log[index-l.index0])
}
func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}
