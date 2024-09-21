package raft

// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool // true if the ApplyMsg contains a newly committed Log entry
	Command      interface{}
	CommandIndex int
	IsLeader     bool
	Term         int
	// For 2D:
	SnapshotValid bool // true if the ApplyMsg contains a snapshot
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.lastApplied = 0
	//if rf.lastApplied+1 <= rf.log.start() {
	//	// reset from a snapshot
	//	rf.lastApplied = rf.log.start()
	//}

	// 只要没有被杀死，就一直循环应用日志
	for !rf.killed() {
		if rf.beginSnapshot {
			am := ApplyMsg{}
			//DPrintf("server %d apply snapshot", rf.me)
			Debug(dSnap, "S%d apply snapshot SnapshotTerm:%v ,SnapshotIndex:%v", rf.me, rf.snapshotTerm, rf.snapshotIndex)
			am.SnapshotValid = true
			am.Snapshot = rf.waitingSnapshot
			am.SnapshotTerm = rf.snapshotTerm
			am.SnapshotIndex = rf.snapshotIndex
			am.CommandValid = false

			//rf.waitingSnapshot = nil
			rf.beginSnapshot = false

			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()

		} else if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.log.lastindex() &&
			rf.lastApplied+1 > rf.log.start() {

			rf.lastApplied++
			rf.persist()

			am := ApplyMsg{}
			am.CommandValid = true
			am.CommandIndex = rf.lastApplied
			am.Command = rf.log.entry(rf.lastApplied).Command

			//DPrintf("server %d apply command %v", rf.me, am.Command)
			Debug(dInfo, "S%d  term:%v,apply CommandIndex：%v, command %v 日志任期：%v", rf.me, rf.currentTerm, am.CommandIndex, am.Command, rf.log.entry(rf.lastApplied).Term)

			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()

		}
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	e := Entry{Term: rf.currentTerm, Command: command}
	index := rf.log.lastindex() + 1
	rf.log.append(e)
	rf.persist()

	Debug(dInfo, "S%d  term:%v, start command %v index %d", rf.me, rf.currentTerm, command, index)

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.sendAppendsL(false)
	}()
	return index, rf.currentTerm, true

}

func (rf *Raft) signalApplierL() {
	rf.applyCond.Broadcast()
}
