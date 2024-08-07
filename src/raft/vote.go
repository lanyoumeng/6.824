package raft

import (
	"math/rand"
	"strconv"
	"time"
)

const selectionTimeout = 1 * time.Second

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //请求选票的候选人的 ID
	LastLogIndex int //候选人的最新日志条目的索引值
	LastLogTerm  int //候选人最新日志条目的任期号
}

// string
func (rva *RequestVoteArgs) String() string {
	return "Term:" + strconv.Itoa(rva.Term) + " CandidateId:" + strconv.Itoa(rva.CandidateId) + " LastLogIndex:" + strconv.Itoa(rva.LastLogIndex) + " LastLogTerm:" + strconv.Itoa(rva.LastLogTerm)

}

// string
func (rvr *RequestVoteReply) String() string {
	return "Term:" + strconv.Itoa(rvr.Term) + " VoteGranted:" + strconv.FormatBool(rvr.VoteGranted)

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

// example RequestVote RPC handler.
// 跟随者一方面要回复候选人的请求，另一方面也要根据请求的内容来决定是否投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dClient, "S%v term:%v RequestVote RPC handler ,args %v", rf.me, rf.currentTerm, args)

	//候选人任期大于节点任期
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}
	//如果term < currentTerm返回 false （5.2 节）
	//如果 votedFor 为空或者为 CandidateId，
	//并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
	myTerm := rf.log.entry(rf.log.lastindex()).Term
	myIndex := rf.log.lastindex()
	uptodate := (args.LastLogTerm == myTerm && args.LastLogIndex >= myIndex) || args.LastLogTerm > myTerm

	//领导者任期小于跟随者节点
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.setElectionTime()

	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm

	rf.persist()

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	// 这里加锁因为上层没有锁，但日志需要读取， 不然会出现并发读写
	rf.mu.Lock()
	Debug(dLeader, "S%v term:%v, sendRequestVote to %v ，args:%v\n", rf.me, rf.currentTerm, server, args)
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发送请求投票
func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, votes *int) {

	rf.mu.Lock()
	////因为是异步发送，所以需要再次判断
	if rf.state != Candidate {
		Debug(dLeader, "S%v not Candidate,return\n", rf.me)
		return
	}
	rf.mu.Unlock()

	var reply RequestVoteReply
	ok := rf.sendRequestVote(peer, args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		Debug(dLeader, "S%v sendRequestVote to %v success,reply: %v\n", rf.me, peer, reply)
		if reply.Term > rf.currentTerm {
			rf.newTermL(reply.Term)
		}

		if rf.currentTerm != args.Term || rf.state != Candidate {
			return
		}

		if reply.VoteGranted {

			// 原来选票<len(rf.peers)/2,选票数+1
			if *votes <= len(rf.peers)/2 {
				*votes++
				if *votes > len(rf.peers)/2 {
					//赢得选举,成为leader，发送心跳
					rf.becomeLeaderL()
					rf.setElectionTime()
					go func() {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						rf.sendAppendsL(true)
					}()
				}
			}
		}

	} else {
		Debug(dLeader, "S%v sendRequestVote to %v false,reply: %v\n", rf.me, peer, ok)
	}

}

// 开始选举
func (rf *Raft) startElectionL() {
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()

	Debug(dClient, "S%v start election， Term %v\n", rf.me, rf.currentTerm)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log.entry(rf.log.lastindex()).Term,
		LastLogIndex: rf.log.lastindex(),
	}

	votes := 1 //得票数

	for i := 0; i < len(rf.peers); i++ {
		i := i
		if i != rf.me {
			go rf.requestVote(i, args, &votes)
		}

	}
}

func (rf *Raft) tick() {
	//这里的锁  只对rf.setElectionTime()有用 ，
	//因为其他函数开了协程导致tick很快执行完毕
	//相当于其他函数上层没有锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dTimer, "S%v tick state: %v,term:%v ,lastindex:%v ,commitindex:%v ,lastApplied:%v ,len(log):%v",
		rf.me, rf.state, rf.currentTerm, rf.log.lastindex(), rf.commitIndex, rf.lastApplied, len(rf.log.Log))

	if rf.state == Leader { //领导者 发送心跳
		rf.setElectionTime()
		rf.sendAppendsL(true) //L表示调用者已经持有锁
	}

	//跟随者/领导者 选举超时，开始新的选举
	if time.Now().After(rf.electionTime) {
		rf.setElectionTime()
		rf.startElectionL()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// 如果这个对等方最近没有收到心跳，ticker go 例程将开始新的选举
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()

		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 设置选举超时
func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(selectionTimeout)
	ms := rand.Int63() % 300
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
	//rf.electionTime = time.Now().Add(time.Duration(rand.Intn(400)+600) * time.Millisecond)
}

func (rf *Raft) newTermL(newterm int) {

	rf.currentTerm = newterm
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()

}

func (rf *Raft) becomeLeaderL() {
	Debug(dClient, "S%v becomeLeader %v ,lastindex:%v\n", rf.me, rf.currentTerm, rf.log.lastindex())
	rf.state = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.lastindex() + 1
	}
	//for i := range rf.matchIndex {
	//	rf.matchIndex[i] = 0
	//}
	rf.persist()
}
