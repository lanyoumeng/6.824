package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

// Clerk user who queries a Key-Value store service
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int   // current leader
	clientId int64 // client id
	seqNo    int   // sequence number
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.clientId = nrand()
	ck.seqNo = 1

	return ck
}

// fetch the current Value for a Key.
// returns "" if the Key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	Debug(dClient, "Get(%v)", key)

	// prepare the arguments.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNo:    ck.seqNo,
	}
	ck.seqNo++

	// send an RPC request, wait for the reply.
	for {
		for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
			reply := GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.leader = i
				return reply.Value
			}
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrNoKey || reply.Err == ErrTimeOut || reply.Err == ErrBackOp {
				Debug(dClient, "Get(%v) failed, retrying...", key)
				continue
			}
			if reply.Err == ErrNoKey {
				return ""
			}
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	Debug(dClient, "%v(%v, %v)", op, key, value)

	// prepare the arguments.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqNo:    ck.seqNo,
	}
	ck.seqNo++

	// send an RPC request, wait for the reply.
	for {
		for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				ck.leader = i
				return
			}
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrNoKey || reply.Err == ErrTimeOut || reply.Err == ErrBackOp {
				Debug(dClient, "%v(%v, %v) failed, retrying...", op, key, value)
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
