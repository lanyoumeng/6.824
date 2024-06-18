package raft

import (
	"log"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
}

func (rf *Raft) sendAppendsL(heartbeat bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		//如果跟随者日志不是最新，或者是心跳包
		if rf.log.lastindex() > rf.nextIndex[i] || heartbeat {
			rf.sendAppendL(i, heartbeat)
		}

	}
}

func (rf *Raft) sendAppendL(peer int, hearthert bool) {
	next := rf.nextIndex[peer]
	if next < rf.log.start() { //总是跳过entry 0
		next = rf.log.start() + 1
	}
	if next > rf.log.lastindex()+1 {
		DPrintf("peer %d next %d > lastindex %d", rf.me, next, rf.log.lastindex())
		next = rf.log.lastindex()
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.log.entry(next - 1).Term,
		Entries:      rf.log.slice(next),
		LeaderCommit: rf.commitIndex,
	}
	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendReplyL(peer, args, &reply)
		}
	}()

}
func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("server %d send append entries to %d args %v", rf.me, peer, args)
	return rf.peers[peer].Call("Raft.AppendEntries", args, reply)
}

// 追加条目（AppendEntries）RPC：
// 由领导人调用，用于日志条目的复制，同时也被当做心跳使用
// 接收者的实现：
//
// 返回假 如果领导人的任期小于接收者的当前任期（译者注：这里的接收者是指跟随者或者候选人）（5.1 节）
// 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在
// prevLogIndex 上能和 prevLogTerm 匹配上 （译者注：在接收者日志中 如果能找到一个和 prevLogIndex
// 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）
// 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），
// 那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
// 追加日志中尚未存在的任何新条目
// 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex）
// ，
// 则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导人的已知已经提交的最高的日志条目的索引
// leaderCommit 或者是 上一个新条目的索引 取两者的最小值
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d receive append entries from %d args %v", rf.me, args.LeaderId, args)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Conflict = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.newTermL(args.Term)

	if args.PrevLogIndex > rf.log.lastindex() {
		reply.Conflict = true
		return
	}
	if args.PrevLogIndex >= rf.log.start() && rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		return
	}

	//删除冲突的日志
	rf.log.cuteStart(args.PrevLogIndex + 1)

	rf.log.log = append(rf.log.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.log.lastindex())
	}
	reply.Success = true
	rf.signalApplierL()
}

// 领导者提交
func (rf *Raft) advanceCommitL() {
	if rf.state != Leader {
		log.Fatal("server %d advance commit not leader", rf.me)

	}
	start := rf.commitIndex + 1
	if start < rf.log.start() { // 从entry 1开始
		start = rf.log.start()
	}

	for index := start; index <= rf.log.lastindex(); index++ {
		if rf.log.entry(index).Term != rf.currentTerm {
			continue
		}
		count := 1 //自己
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= index {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			DPrintf("server %d advance commit index %d", rf.me, index)
			rf.commitIndex = index
		}

	}
	rf.signalApplierL()
}

func (rf *Raft) processAppendReplyTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		newnext := args.PrevLogIndex + len(args.Entries) + 1
		newmatch := args.PrevLogIndex + len(args.Entries)
		if newnext > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newnext

		}
		if newmatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newmatch
		}
		DPrintf("server %d append success to %d next %d match %d", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
	} else if reply.Conflict {
		rf.processConflictL(peer, args, reply)
		//rf.sendAppendL(peer, false)
	} else if rf.nextIndex[peer] > 1 {

		DPrintf("server %d append fail to %d next %d", rf.me, peer, rf.nextIndex[peer])
		rf.nextIndex[peer]--
		if rf.nextIndex[peer] < rf.log.start()+1 {
			rf.sendsnapshot(peer)
		}

	}

	rf.advanceCommitL() //提交日志
}
func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("server %d process append reply from %d args %v reply %v", rf.me, peer, args, reply)
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
	} else if reply.Term == rf.currentTerm {
		rf.processAppendReplyTermL(peer, args, reply)
	}

}
