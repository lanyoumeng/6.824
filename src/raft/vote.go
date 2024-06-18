package raft

import (
	"6.824/labrpc"
	"math/rand"
	"sync"
	"time"
)

const selectionTimeout = 1 * time.Second

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int //候选人的任期号
	candidateId  int //请求选票的候选人的 ID
	lastLogIndex int //候选人的最后日志条目的索引值
	lastLogTerm  int //候选人最后日志条目的任期号
}

func (rva *RequestVoteArgs) String() string {
	return "term:" + string(rva.term) + " candidateId:" + string(rva.candidateId) + " lastLogIndex:" + string(rva.lastLogIndex) + " lastLogTerm:" + string(rva.lastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	term        int  //当前任期号，以便于候选人去更新自己的任期号
	voteGranted bool //候选人赢得了此张选票时为真
}

// example RequestVote RPC handler.
// 跟随者一方面要回复候选人的请求，另一方面也要根据请求的内容来决定是否投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v: RequestVote %v %v\n", rf.me, args, rf.currentTerm)

	if args.term > rf.currentTerm {
		rf.newTermL(args.term)
	}
	//如果term < currentTerm返回 false （5.2 节）
	//如果 votedFor 为空或者为 candidateId，
	//并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
	myTerm := rf.log.entry(rf.log.lastindex()).Term
	myIndex := rf.log.lastindex()
	uptodate := args.lastLogTerm > myTerm || (args.lastLogTerm == myTerm && args.lastLogIndex >= myIndex)
	if args.term < rf.currentTerm {
		reply.voteGranted = false

	} else if uptodate && (rf.votedFor == -1 || rf.votedFor == args.candidateId) {
		rf.votedFor = args.candidateId
		reply.voteGranted = true
		rf.persist()
		rf.setElectionTime()
	} else {
		reply.voteGranted = false

	}

	reply.term = rf.currentTerm

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
	DPrintf("%v: sendRequestVote to %v %v\n", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) newTermL(newterm int) {
	rf.currentTerm = newterm
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()

}

func (rf *Raft) becomeLeaderL() {
	DPrintf("%v: becomeLeader %v %v\n", rf.me, rf.currentTerm, rf.log.lastindex())
	rf.state = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.lastindex() + 1
	}
}

func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(peer, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("server %d request vote from %d reply %v", rf.me, peer, reply)

		if reply.term > rf.currentTerm {

			rf.newTermL(reply.term)
		}

		if reply.voteGranted {
			*votes++
			if *votes > len(rf.peers)/2 {
				//赢得选举,成为leader，发送心跳
				//rf.state = Leader
				rf.becomeLeaderL()
				//rf.setElectionTime()
				rf.sendAppendsL(true)
			}
		}
	}

}
func (rf *Raft) requestVotesL() {

	args := &RequestVoteArgs{
		term:         rf.currentTerm,
		candidateId:  rf.me,
		lastLogIndex: rf.log.lastindex(),
		lastLogTerm:  rf.log.entry(rf.log.lastindex()).Term,
	}

	votes := 1 //得票数

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			go rf.requestVote(i, args, &votes)
		}

	}
}
func (rf *Raft) startElectionL() {
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()

	DPrintf("server %d start election term %d", rf.me, rf.currentTerm)

	rf.requestVotesL()
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("server %d tick state %v", rf.me, rf.state)
	if rf.state == Leader {
		rf.setElectionTime()
		rf.sendAppendsL(true) //L表示调用者已经持有锁
	}

	if time.Now().After(rf.electionTime) {
		rf.setElectionTime()
		rf.startElectionL()
	}

}

// 设置选举超时
func (rf *Raft) setElectionTime() {
	rf.electionTime = time.Now().Add(time.Duration(rand.Intn(300)+1000) * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.state = Follower
	rf.setElectionTime()

	rf.votedFor = -1
	rf.log = mkLogEmpty()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applier()
	// start ticker goroutine to start elections
	//心跳+选举
	go rf.ticker()

	return rf
}
