package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind  string
	Key   string
	Value string
	Id    int64
	Reqid int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db     map[string]string
	result map[int]chan Op
	ack    map[int64]int
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {
	// DPrintf("%v\n", kv.me)
	index, _, isLeader := kv.rf.Start(entry)
	DPrintf("%v: %v\n", kv.me, isLeader)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (kv *RaftKV) CheckDup(id int64, reqid int) bool {
	v, ok := kv.ack[id]
	if ok {
		return v >= reqid
	} else {
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// Get 也提交日志防止此 Leader 在少数分区中。
	entry := Op{Kind: "Get", Key: args.Key, Id: args.Id, Reqid: args.Reqid}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		DPrintf("%v: Get: %v\n", kv.me, args.Key)
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.ack[args.Id] = args.Reqid
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{Kind: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, Reqid: args.Reqid}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		DPrintf("%v: PutApppend: %v, Value: %v\n", kv.me, args.Key, args.Value)
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Err = OK
		kv.ack[args.Id] = args.Reqid
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Apply(args Op) {
	DPrintf("%v: Apply: %v-%v-%v\n", kv.me, args.Kind, args.Key, args.Value)
	switch args.Kind {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.Id] = args.Reqid
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.db = make(map[string]string)
	kv.result = make(map[int]chan Op)
	kv.ack = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh
			if msg.UseSnapshot {
				var LastIncludedIndex int
				var LastIncludedTerm int

				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.db = make(map[string]string)
				kv.ack = make(map[int64]int)
				d.Decode(&kv.db)
				d.Decode(&kv.ack)
				kv.mu.Unlock()
			} else {
				op := msg.Command.(Op)
				kv.mu.Lock()
				if !kv.CheckDup(op.Id, op.Reqid) {
					kv.Apply(op)
				}
				ch, ok := kv.result[msg.Index]
				if ok {
					select {
					case <-ch:
					default:
					}
					ch <- op
				} else {
					kv.result[msg.Index] = make(chan Op, 1)
				}
				if kv.maxraftstate != -1 && kv.rf.GetPerisistSize() > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.ack)
					data := w.Bytes()
					go kv.rf.StartSnapshot(data, msg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
