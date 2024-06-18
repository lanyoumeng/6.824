package raft

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
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

	DPrintf("server %d start command %v index %d", rf.me, command, index)

	rf.sendAppendsL(false)
	return index, rf.currentTerm, true

}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = 0

	// 如果lastApplied < snapshotIndex，那么就加载快照
	if rf.lastApplied+1 <= rf.log.start() {
		// reset from a snapshot
		rf.lastApplied = rf.log.start()

	}

	for !rf.killed() {
		if rf.waitingSnapshot != nil {
			am := ApplyMsg{}
			DPrintf("server %d apply snapshot", rf.me)
			am.SnapshotValid = true
			am.Snapshot = rf.waitingSnapshot
			am.SnapshotTerm = rf.waitingTerm
			am.SnapshotIndex = rf.waitingIndex

			rf.waitingSnapshot = nil

			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()

		} else if rf.lastApplied+1 < rf.commitIndex &&
			rf.lastApplied+1 <= rf.log.lastindex() &&
			rf.lastApplied+1 > rf.log.start() {

			rf.lastApplied++

			am := ApplyMsg{}
			am.CommandValid = true
			am.CommandIndex = rf.lastApplied
			am.Command = rf.log.entry(rf.lastApplied).Command

			DPrintf("server %d apply command %v", rf.me, am.Command)

			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()

		}
	}

}

func (rf *Raft) signalApplierL() {
	rf.applyCond.Broadcast()
}
