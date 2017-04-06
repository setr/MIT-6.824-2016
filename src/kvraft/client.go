package raftkv

// import "fmt"
import "sync"
import "labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
    servers []*labrpc.ClientEnd
    // You will have to modify this struct.
    id int64
    reqid int
    mu sync.Mutex
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
    ck.id = nrand()
    ck.reqid = 0
    return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

    // You will have to modify this function.
    var args GetArgs
    args.Key = key
    args.Id = ck.id
    args.Reqid = ck.reqid

    ck.mu.Lock()
    ck.reqid++
    ck.mu.Unlock()

    for {
        for _, v := range ck.servers {
            var reply GetReply
            ok := v.Call("RaftKV.Get", &args, &reply)
            // fmt.Printf("Get WrongLeader: %v\n", reply.WrongLeader)
            if ok && reply.WrongLeader == false {
                // fmt.Printf("%d: Get: %v-%v\n", ck.id, key, reply.Value)
                return reply.Value
            }
        }
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    // You will have to modify this function.
    // fmt.Printf("Put Append\n")
    var args PutAppendArgs
    args.Key = key
    args.Value = value
    args.Id = ck.id
    args.Op = op
    args.Reqid = ck.reqid

    ck.mu.Lock()
    ck.reqid++
    ck.mu.Unlock()

    for {
        for _, v := range ck.servers {
            var reply PutAppendReply
            ok := v.Call("RaftKV.PutAppend", &args, &reply)
            if ok && reply.WrongLeader == false {
                // fmt.Printf("%d: Put success: %v %v-%v\n", ck.id, ck.reqid, key, value)
                return
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
