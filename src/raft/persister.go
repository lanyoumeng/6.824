package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"6.824/labgob"
	"bytes"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 将 Raft 的持久 化状态 保存到稳定存储中，以后可以在崩溃后重新找回。
// 请参阅论文的图 2，了解应保持性的内容。
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.beginSnapshot)
	//
	//e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	//e.Encode(rf.nextIndex)
	//e.Encode(rf.matchIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 {
		return

	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log

	var snapshotIndex int
	var snapshotTerm int
	var beginSnapshot bool
	//
	//var commitIndex int
	var lastApplied int
	//var nextIndex []int
	//var matchIndex []int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil ||
		d.Decode(&beginSnapshot) != nil ||
		//d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil {
		//d.Decode(&nextIndex) != nil ||
		//d.Decode(&matchIndex) != nil {

		panic("raft/readPersist decode err")

	} else {
		// 恢复元数据
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		// 恢复快照元数据
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.beginSnapshot = beginSnapshot

		//rf.commitIndex = snapshotIndex
		rf.lastApplied = snapshotIndex

		//// 恢复日志元数据
		//if rf.lastApplied+1 <= rf.log.start() {
		//	// reset from a snapshot
		//	rf.lastApplied = rf.log.start()
		//}

		//rf.commitIndex = commitIndex
		//rf.lastApplied = lastApplied
		//rf.nextIndex = nextIndex
		//rf.matchIndex = matchIndex

	}
}
