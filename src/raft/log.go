package raft

import "fmt"

// Entry 结构体表示日志中的一条记录，包含任期号和命令
type Entry struct {
	Term    int         // 任期号
	Command interface{} // 命令，可以是任意类型
}

// String 方法实现了 Entry 结构体的字符串表示形式
func (e Entry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", e.Term, e.Command)
}

// Log 结构体表示一系列的日志条目
type Log struct {
	Log    []Entry // 日志条目切片，存储实际的日志条目  索引从Index0开始计算
	Index0 int     // 日志的起始索引
}

// mkLogEmpty 函数创建一个空的日志对象
func mkLogEmpty() Log {
	return Log{Log: make([]Entry, 1), Index0: 0} // 初始化一个包含一个空 Entry 的日志切片
}

// mkLog 函数根据给定的日志条目和起始索引创建一个日志对象
func mkLog(log []Entry, index0 int) Log {
	return Log{Log: log, Index0: index0}
}

// append 方法向日志追加一个新的条目
func (l *Log) append(entry Entry) {
	l.Log = append(l.Log, entry)
}

// start 方法返回日志的起始索引
func (l *Log) start() int {
	return l.Index0
}

// lastindex 方法返回最新的日志索引
func (l *Log) lastindex() int {
	return l.Index0 + len(l.Log) - 1
}

// entry 方法返回指定索引处的日志条目指针 没有则返回 nil
func (l *Log) entry(index int) *Entry {
	if index < l.Index0 || index >= l.Index0+len(l.Log) {
		Debug(dError, "index0:%v , l.Index0+len(l.Log):%v, index:%v", l.Index0, l.Index0+len(l.Log), index)
		return nil // 或者 panic，取决于具体需求
	}
	return &(l.Log[index-l.Index0])
}

// lastentry 方法返回最新的日志条目指针
func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}

// cutend 方法截断日志，使其只包含到指定索引-1的条目
func (l *Log) cutend(index int) {
	l.Log = l.Log[0 : index-l.Index0]
}

// cuteStart 方法从指定索引开始截断日志，更新日志的起始索引
func (l *Log) cuteStart(index int) {
	l.Index0 += index
	l.Log = l.Log[index:]
}

// slice 方法返回从指定索引开始的所有日志条目
func (l *Log) slice(index int) []Entry {
	if index < l.Index0 {
		return nil

	}

	return l.Log[index-l.Index0:]
}
