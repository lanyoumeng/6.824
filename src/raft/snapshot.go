package raft

import (
	"6.824/labgob"
	"bytes"
	"strconv"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

// string
func (args InstallSnapshotArgs) String() string {
	return "Term:" + strconv.Itoa(args.Term) + " LeaderId:" + strconv.Itoa(args.LeaderId) + " LastIncludedIndex:" + strconv.Itoa(args.LastIncludedIndex) + " LastIncludedTerm:" + strconv.Itoa(args.LastIncludedTerm) + " Snapshotlen:" + strconv.Itoa(len(args.Snapshot))

}

// InstallSnapshot RPC handler.
// 主节点从上层应用接收到快照请求（压缩日志），表示上层应用已经创建了一个上层应用的快照
// 然后主节点将快照和元数据持久化到磁盘，并更新自己的状态

// Snapshot 的数据是上层应用传递给 Raft 主节点的，Raft 主节点会将这个数据传递给其他节点。
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	//  如果快照小于提交的commitIndex或者小于当前快照索引，那么直接返回
	if args.LastIncludedIndex <= rf.commitIndex || args.LastIncludedIndex <= rf.snapshotIndex {
		Debug(dSnap, "S%d 收到快照请求，args:%v,但是快照小于提交的commitIndex或者小于当前快照索引 ,rf.commitIndex:%v ,rf.snapshotIndex:%v", rf.me, args, rf.commitIndex, rf.snapshotIndex)
		return
	}

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}
	reply.Term = rf.currentTerm
	rf.setElectionTime()
	rf.state = Follower

	Debug(dSnap, "S%d 收到快照请求，args:%v", rf.me, args)

	////压缩日志
	//peer第一条日志是快照的最后一条日志
	if args.LastIncludedIndex > rf.log.lastindex() {
		// 情况1: LastIncludedIndex 大于当前日志的最大索引，创建一个新的日志，只有快照的最后一条日志
		rf.log = mkLog([]Entry{{Term: args.LastIncludedTerm}}, args.LastIncludedIndex)
	} else if args.LastIncludedIndex >= rf.log.start() {
		// 情况2: LastIncludedIndex 在当前日志范围内，截取该索引之后的日志
		rf.log = mkLog(rf.log.slice(args.LastIncludedIndex), args.LastIncludedIndex)
	} else {
		// 情况3: LastIncludedIndex 小于当前日志的起始索引，创建一个新的日志，只有快照的最后一条日志
		rf.log = mkLog([]Entry{{Term: args.LastIncludedTerm}}, args.LastIncludedIndex)
	}

	//注意截取后要进行覆盖  不然原来args.LastIncludedIndex处的日志还在
	rf.log.Log[0].Command = nil
	rf.log.Log[0].Term = args.LastIncludedTerm

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshotIndex = args.LastIncludedIndex
	rf.waitingSnapshot = args.Snapshot
	rf.beginSnapshot = true

	// 保存快照数据
	rf.SnapshotL(args.LastIncludedIndex, args.Snapshot)

	// 通知应用层有新的快照需要处理
	rf.applyCond.Signal()
}

func (rf *Raft) sendSnapshot(peer int) {

	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Snapshot:          rf.waitingSnapshot,
	}
	var reply InstallSnapshotReply

	Debug(dSnap, "S%d 发送快照到 S%d,  rf.nextIndex[peer]:%v ,args:%v", rf.me, peer, rf.nextIndex[peer], args)
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.processInstallSnapshotReplyL(peer, &args, &reply)

	}
}

func (rf *Raft) sendInstallSnapshot(peer int, i *InstallSnapshotArgs, i2 *InstallSnapshotReply) bool {
	return rf.peers[peer].Call("Raft.InstallSnapshot", i, i2)
}

func (rf *Raft) processInstallSnapshotReplyL(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
		return
	}
	if args.Term != rf.currentTerm {
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[peer] {

		rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex) + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1

		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.sendAppendL(peer, false)
		}()

	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
//该服务表示，它已经创建了一个快照，其中包含所有信息，包括索引。这意味着服务
//不再需要通过该索引(并包括该索引)的日志。Raft 现在应该尽可能地修剪其 Log。

// 主节点从上层应用接收到快照请求
// 丢弃 index 之前的日志条目。
// 更新 SnapshotIndex 和 SnapshotTerm 以反映快照的状态。
// 使用 persister.Save() 持久化当前状态和快照。
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果index小于等于快照索引，则不需要快照
	if index <= rf.snapshotIndex {
		return
	}
	Debug(dSnap, "S%d SnapshotL index:%d ,snapshotlen:%v", rf.me, index, len(snapshot))
	k := rf.log.entry(index)
	// 更新快照索引和快照任期还有数据
	rf.snapshotTerm = k.Term
	rf.snapshotIndex = index
	rf.waitingSnapshot = snapshot
	rf.beginSnapshot = true //

	// 丢弃index之前的日志条目
	//这里index处的日志也是没用的，因为index处的日志是快照的最后一条日志
	rf.log = mkLog(rf.log.slice(index), index)
	//将lastApplied设置为index，之后的应用从index+1开始
	rf.lastApplied = index

	//不对，应为只有主节点才能设置提交，但Snapshot各个节点都被调用
	//因为延迟可能导致从节点先处理日志，刷新了commitIndex，所以这里需要重新计算commitIndex
	//rf.advanceCommitL()

	// 保存快照数据
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
	rf.persister.Save(raftstate, snapshot)

	if len(rf.log.Log) > 0 {
		Debug(dSnap, "S%d snapshots at index:%d Log[%d - %d]", rf.me, index, rf.log.start(), rf.log.lastindex())
	}

}

func (rf *Raft) SnapshotL(index int, snapshot []byte) {

	//如果index小于等于快照索引，则不需要快照
	if index <= rf.snapshotIndex {
		return
	}
	Debug(dSnap, "S%d SnapshotL index:%d ,snapshotlen:%v", rf.me, index, len(snapshot))
	k := rf.log.entry(index)
	// 更新快照索引和快照任期还有数据
	rf.snapshotTerm = k.Term
	rf.snapshotIndex = index
	rf.waitingSnapshot = snapshot
	rf.beginSnapshot = true

	// 丢弃index之前的日志条目
	//这里index处的日志也是没用的，因为index处的日志是快照的最后一条日志
	rf.log = mkLog(rf.log.slice(index), index)
	//将lastApplied设置为index，之后的应用从index+1开始
	rf.lastApplied = index

	// 保存快照数据
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.beginSnapshot)

	//e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	//e.Encode(rf.nextIndex)
	//e.Encode(rf.matchIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	if len(rf.log.Log) > 0 {
		Debug(dSnap, "S%d snapshots at index:%d Log[%d - %d]", rf.me, index, rf.log.start(), rf.log.lastindex())
	}
}
