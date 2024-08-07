package raft

import (
	"log"
	"strconv"
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
	Term          int
	Success       bool
	ConflictIndex int // 冲突的日志条目的第一条索引
	ConflictTerm  int // 冲突的日志条目的任期 prevLogIndex 不存在则为-1
}

// string
func (args *AppendEntriesArgs) String() string {
	return "Term:" + strconv.Itoa(args.Term) + " LeaderId:" + strconv.Itoa(args.LeaderId) + " PrevLogIndex:" + strconv.Itoa(args.PrevLogIndex) + " PrevLogTerm:" + strconv.Itoa(args.PrevLogTerm) + " LeaderCommit:" + strconv.Itoa(args.LeaderCommit) + " Entrieslen:" + strconv.Itoa(len(args.Entries))

}

// string
func (reply *AppendEntriesReply) String() string {
	return "Term:" + strconv.Itoa(reply.Term) + " Success:" + strconv.FormatBool(reply.Success) + " ConflictIndex:" + strconv.Itoa(reply.ConflictIndex) + " ConflictTerm:" + strconv.Itoa(reply.ConflictTerm)

}

// AppendEntries RPC handler. 附加日志和心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dClient, "S%d 对等体处理追加日志rpc请求1:receive append entries from %d ,args: %v", rf.me, args.LeaderId, args)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1

	// 返回假 如果领导人的任期小于接收者的当前任期（译者注：这里的接收者是指跟随者或者候选人）（5.1 节）
	if args.Term < rf.currentTerm {
		Debug(dClient, "S%d 对等体处理追加日志rpc请求2:receive append entries from %d ,args: %v, term < currentTerm", rf.me, args.LeaderId, args)
		return
	}

	rf.setElectionTime()
	rf.state = Follower
	// 如果领导人的任期大于接收者的当前任期，那么接收者就变成跟随者（5.1 节）
	if args.Term > rf.currentTerm {
		Debug(dClient, "S%d 对等体处理追加日志rpc请求3:receive append entries from %d ,args: %v, term > currentTerm", rf.me, args.LeaderId, args)
		rf.newTermL(args.Term)
	}

	//（译者注：在接收者日志中 如果能找到一个和 prevLogIndex以及 prevLogTerm
	//一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）

	//1.prevLogIndex 不存在：如果跟随者的日志中没有 prevLogIndex，
	//那么它应该返回 ConflictIndex = len(log)，并且 conflictTerm = None。
	if rf.log.entry(args.PrevLogIndex) == nil {
		//reply.ConflictIndex = rf.log.lastindex() + 1
		reply.ConflictIndex = rf.log.lastindex()
		Debug(dClient, "S%d 对等体处理追加日志rpc请求4 ，prevLogIndex not exist, from %d , lastindex:%v ,reply: %v,", rf.me, args.LeaderId, rf.log.lastindex(), reply)

		rf.persist()
		return
	}

	//2.prevLogIndex 存在但任期不匹配：
	//如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同）
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		//返回 conflictTerm = log[prevLogIndex].Term，
		reply.ConflictTerm = rf.log.entry(args.PrevLogIndex).Term
		//然后在其日志中搜索第一个任期等于 conflictTerm 的条目的索引。
		for i := args.PrevLogIndex; i >= rf.log.start(); i-- {
			if rf.log.entry(i).Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		// 删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
		rf.log.cutend(args.PrevLogIndex)
		rf.persist()

		Debug(dClient, "S%d 对等体处理追加日志rpc请求5: prevLogIndex 存在但任期不匹配：请求来自S%d ,args: %v, ", rf.me, args.LeaderId, args)
		return
	}

	// 删除多余日志 然后再追加日志中尚未存在的任何新条目
	//
	for i := 1; i <= len(args.Entries); i++ {
		if args.PrevLogIndex+i <= rf.log.lastindex() && rf.log.entry(args.PrevLogIndex+i).Term != args.Entries[i-1].Term {
			rf.log.cutend(args.PrevLogIndex + i)
		}
		if args.PrevLogIndex+i > rf.log.lastindex() {
			rf.log.Log = append(rf.log.Log, args.Entries[i-1])

		}
	}

	// 不对，直接删除args.PrevLogIndex + 1之后的日志
	//会导致args.PrevLogIndex合法但有日志更短的节点，会导致日志不一致（比如延迟到达但更短的）
	//rf.log.cutend(args.PrevLogIndex + 1)
	//rf.log.Log = append(rf.log.Log, args.Entries...)

	// 如果领导人的已知已提交的最高日志条目的索引大于
	//接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex）
	// ， 则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为
	//领导人的已知已经提交的最高的日志条目的索引
	// leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastindex())
		rf.signalApplierL()
	}

	reply.Success = true
	rf.persist()
	Debug(dClient, "S%d currentTerm:%v,AppendEntries RPC handler:添加日志成功 from %d ,args: %v", rf.me, rf.currentTerm, args.LeaderId, args)
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// 这里加锁因为上层没有锁，但日志需要读取， 不然会出现并发读写
	rf.mu.Lock()
	Debug(dLeader, "S%d ,state:%v, currentTerm:%v，发送追加日志rpc到 %d , args:%v", rf.me, rf.state, rf.currentTerm, peer, args)
	rf.mu.Unlock()

	return rf.peers[peer].Call("Raft.AppendEntries", args, reply)
}

// 上层很快解锁
func (rf *Raft) sendAppendsL(heartbeat bool) {

	for i := range rf.peers {
		i := i
		if i != rf.me {
			//跟随者日志不是最新或者是心跳
			if rf.log.lastindex() > rf.nextIndex[i] || heartbeat {

				go func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.sendAppendL(i, heartbeat)
				}()

			}
		}
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {

	// 领导者才能发送
	//因为是异步发送，所以需要再次判断（比如上层for循环，节点已经不是领导者）
	// 上层判断不精确
	if rf.state != Leader {
		Debug(dTimer, "S%d 不是领导者，发送追加日志失败 to %d,currentTerm:%v ,state:%v", rf.me, peer, rf.currentTerm, rf.state)
		return
	}

	// 如果是心跳
	if heartbeat == true {
		Debug(dTimer, "S%d 发送心跳 to %d,currentTerm:%v ,state:%v", rf.me, peer, rf.currentTerm, rf.state)
	}

	next := rf.nextIndex[peer]

	//发送快照
	if (next <= rf.snapshotIndex || next <= rf.log.start()) && rf.snapshotIndex > 0 {
		go rf.sendSnapshot(peer)
		return
	}

	if next <= rf.log.start() {
		next = rf.log.start() + 1
	}

	if next-1 > rf.log.lastindex() {
		next = rf.log.lastindex()
	}

	//发送心跳
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

// 处理追加日志或心跳rpc回复 判断回复任期
func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	Debug(dLeader, "S%d currentTerm:%v ,处理追加日志响应--来自节点 %d ,args: [%v] ,reply: [%v]", rf.me, rf.currentTerm, peer, args, reply)

	// 1.如果领导人的任期小于接收者的当前任期
	// 2.当前节点已经不是领导者
	//
	//if reply.Term > rf.currentTerm || rf.state != Leader {
	//	Debug(dLeader, "S%d currentTerm:%v ,变为跟随者， 追加日志失败 to %d ,reply.Term:%v", rf.me, rf.currentTerm, peer, reply.Term)
	//	rf.newTermL(reply.Term)
	//
	//} else if reply.Term == rf.currentTerm {
	//	rf.processAppendReplyTermL(peer, args, reply)
	//} else {
	//	Debug(dLeader, "S%d currentTerm:%v ,processAppendReplyL else,追加日志失败 to %d ,reply.Term:%v", rf.me, rf.currentTerm, peer, reply.Term)
	//}

	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
		return
	}

	if rf.currentTerm != args.Term {
		return
	}
	if args.PrevLogIndex != rf.nextIndex[peer]-1 {
		return
	}

	rf.processAppendReplyTermL(peer, args, reply)

}

// 处理追加日志或心跳rpc回复 回复任期正确
func (rf *Raft) processAppendReplyTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if reply.Success {

		if len(args.Entries) > 0 {
			Debug(dLeader, "S%d currentTerm:%v  追加日志成功 to %d ,nextIndex: %d ,matchIndex: %d", rf.me, rf.currentTerm, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		}

		newnext := args.PrevLogIndex + len(args.Entries) + 1
		newmatch := args.PrevLogIndex + len(args.Entries)
		if newnext > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newnext
		}
		if newmatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newmatch
		}

		rf.advanceCommitL() //提交日志

	} else {

		//领导者处理冲突响应：领导者收到冲突响应后，应该首先在其日志中搜索 conflictTerm
		//1.如果找到具有该任期的条目，它应该将 nextIndex 设置为日志中最后一个该任期条目之后的索引。
		//2.如果没有找到具有该任期的条目，则应将 nextIndex 设置为 ConflictIndex。
		//然后，领导者应该重试其 AppendEntries RPC（5.3 节）
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			found := false
			for i := rf.log.lastindex(); i >= rf.log.start(); i-- {
				if rf.log.entry(i).Term == reply.ConflictTerm {
					rf.nextIndex[peer] = i + 1
					found = true
					break
				}
			}
			if !found {
				rf.nextIndex[peer] = reply.ConflictIndex
			}
		}

		rf.persist()

		//冲突后重新发送 一定是领导者
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			Debug(dLeader, "S%d currentTerm:%v 追加日志冲突,重新发送追加日志 to %d ,nextIndex: %d", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
			rf.sendAppendL(peer, false)
		}()

	}

}

// 领导者提交日志并持久化 向对等体追加日志
func (rf *Raft) advanceCommitL() {
	if rf.state != Leader {
		log.Fatalf("server %d advance commit not leader", rf.me)

	}
	start := rf.commitIndex + 1
	if start < rf.log.start() { // 从entry 1开始
		start = rf.log.start()
	}

	for index := start; index <= rf.log.lastindex(); index++ {
		//Leader不能直接提交小于当前Term的日志，只能通过提交当前Term的日志间接提交先前日志
		if rf.log.entry(index).Term != rf.currentTerm {
			continue
		}
		count := 1 //自己
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			Debug(dCommit, "S%d term:%v, advance commit index %d,command:%v", rf.me, rf.currentTerm, index, rf.log.entry(index).Command)
			rf.commitIndex = index

		} else {
			break
		}

	}
	rf.persist()
	//rf.signalApplierL()

	if rf.commitIndex > rf.lastApplied {
		rf.applyCond.Signal()

		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.sendAppendsL(false)
		}()
	}
}
