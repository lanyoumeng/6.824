package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// 客户端请求的操作,也就是command
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	IsGet       bool
	IsPutAppend bool
	PutAppend   string // "Put" or "Append"
	Key         string
	Value       string
	ClientId    int64
	SeqNo       int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	data    map[string]string // Key-Value data
	seqNo   map[int64]int     // client id -> sequence number
	nowsize int               // current raft state size

	watchCh map[int64]chan Op //  用来等待raft处理完毕之后，监控读写key-value的操作

	presist *raft.Persister // persister

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	// 如果已经处理过这个请求，直接返回
	if kv.seqNo[args.ClientId] >= args.SeqNo {
		reply.Value = kv.data[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	// 未处理过，提交到raft
	// 成功会在raft的applyCh中返回
	// 因为要保证线性一致性，所以需要一个单独的协程来等待raft处理完毕
	op := Op{
		IsGet:    true,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}

	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待raft处理，
	kv.mu.Lock()
	// 如果没有watchCh，初始化
	kv.watchCh[args.ClientId] = make(chan Op, 1)
	watchCh := kv.watchCh[args.ClientId]

	kv.mu.Unlock()

	select {
	case <-time.After(500 * time.Millisecond):
		// 超时
		kv.mu.Lock()
		delete(kv.watchCh, args.ClientId)
		kv.mu.Unlock()

		reply.Err = ErrTimeOut
		return
	case back := <-watchCh:

		// 处理完毕,删除watchCh
		kv.mu.Lock()
		delete(kv.watchCh, args.ClientId)
		kv.mu.Unlock()

		// 因为是并发的，所以需要再次检查
		//客户端协程拿锁注册 watchCh 和 apply 协程拿锁执行该日志再进行 watchCh 之间的拿锁顺序无法绝对保证
		// 假如客户端协程拿到锁注册 watchCh 之后，apply 协程拿到锁执行该日志，
		//当前client又有一个请求注册 watchCh，那么客户端协程的 watchCh 就会被 apply 协程的 watchCh 覆盖
		if back.Key != args.Key || back.ClientId != args.ClientId || back.SeqNo != args.SeqNo {
			reply.Err = ErrBackOp
			return
		}
		reply.Value = back.Value
		reply.Err = OK
		return

	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()

	// 如果已经处理过这个请求，直接返回
	if kv.seqNo[args.ClientId] >= args.SeqNo {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	// 未处理过，提交到raft
	// 成功会在raft的applyCh中返回
	// 因为要保证线性一致性，所以需要一个单独的协程来等待raft处理完毕
	op := Op{
		IsPutAppend: true,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SeqNo:       args.SeqNo,
		PutAppend:   args.Op,
	}

	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待raft处理，
	kv.mu.Lock()
	// 如果没有watchCh，初始化
	kv.watchCh[args.ClientId] = make(chan Op, 1)
	watchCh := kv.watchCh[args.ClientId]
	kv.mu.Unlock()

	select {
	case <-time.After(500 * time.Millisecond):
		// 超时
		kv.mu.Lock()
		delete(kv.watchCh, args.ClientId)
		kv.mu.Unlock()

		reply.Err = ErrTimeOut
		return
	case back := <-watchCh:
		kv.mu.Lock()
		delete(kv.watchCh, args.ClientId)
		kv.mu.Unlock()

		if back.Key != args.Key || back.ClientId != args.ClientId || back.SeqNo != args.SeqNo {
			reply.Err = ErrBackOp
			return
		}
		reply.Err = OK
		return

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant Key/Value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.seqNo = make(map[int64]int)
	kv.nowsize = 0
	kv.watchCh = make(map[int64]chan Op)

	// snapshot
	kv.presist = persister
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.apply()

	return kv
}

// snapshot
func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var seqNo map[int64]int

	if d.Decode(&data) != nil || d.Decode(&seqNo) != nil {
		log.Fatal("readSnapshot error")
	} else {
		kv.data = data
		kv.seqNo = seqNo

	}
}

func (kv *KVServer) saveSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.seqNo)
	data := w.Bytes()

	go kv.rf.Snapshot(index, data)

}

func (kv *KVServer) apply() {
	for {
		if kv.killed() {
			return
		}
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)

			kv.mu.Lock()
			if kv.seqNo[op.ClientId] < op.SeqNo {
				kv.seqNo[op.ClientId] = op.SeqNo

				if op.IsGet {
					// Get 请求不需要修改数据，因此可以在不锁住全局锁的情况下执行
					value := kv.data[op.Key]
					op.Value = value
				} else if op.IsPutAppend {
					// 处理 Put 或 Append
					if op.PutAppend == "Put" {
						kv.data[op.Key] = op.Value
					} else if op.PutAppend == "Append" {
						kv.data[op.Key] += op.Value
					}
				}

				// 响应客户端操作
				select {
				case kv.watchCh[op.ClientId] <- op:
				default:
					// 可以增加日志记录或重试机制，防止操作丢失
				}

				// 更新快照状态
				kv.nowsize += calculateOpSize(op)
				if kv.maxraftstate > 0 && kv.nowsize > kv.maxraftstate {
					kv.saveSnapshot(msg.CommandIndex)
					kv.nowsize = 0
				}
			}
			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func calculateOpSize(op Op) int {
	// 更准确的内存计算方法
	return int(unsafe.Sizeof(Op{})) + len(op.Key) + len(op.Value) + 8
}
