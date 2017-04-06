package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
    "sync"
    "labrpc"
    "time"
    "bytes"
    "encoding/gob"
    "math/rand"
)

const (
    STATE_LEADER = iota
    STATE_CANDIDATE
    STATE_FLLOWER

    HBINTERVAL = 50 * time.Millisecond // 50ms
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
    Index       int
    Command     interface{}
    UseSnapshot bool   // ignore for lab2; only used in lab3
    // Snapshot 没有实现
    Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
    LogTerm int // Log 提交 term
    LogComd interface{} // Log 原始命令
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex // 锁
    peers     []*labrpc.ClientEnd // 节点之间相互关联
    persister *Persister // 持久化
    me        int // index into peers[]

    // Your data here.
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    currentTerm int // 当前 term
    voteFor int // 给某一节点投票，未投票为 -1
    voteCount int  // 获取票数
    state int // 当前状态
    chanCommit chan bool // 已提交数据通道
    chanHeartbeat chan bool // 心跳包通道
    chanGrantVote chan bool // 投票通道
    chanLeader chan bool // 领导者选举成功通道
    chanApply chan ApplyMsg // 命令提交通道

    log []LogEntry // 所有日志

    commitIndex int // 可提交日志最大标号
    lastApplied int // 已提交日志编号

    nextIndex []int // 每个节点需要同步的日志最小编号
    matchIndex []int //每个节点已经匹配的编号
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    return rf.currentTerm, rf.state == STATE_LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here.
    // Example:
    // w := new(bytes.Buffer)
    // e := gob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.voteFor)
    e.Encode(rf.log)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    // Your code here.
    // Example:
    // r := bytes.NewBuffer(data)
    // d := gob.NewDecoder(r)
    // d.Decode(&rf.xxx)
    // d.Decode(&rf.yyy)
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.voteFor)
    d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    // Your data here.
    Term int // 当前 Term
    CandidateId int // 请求投票节点 id
    LastLogIndex int // 请求投票节点日志最大编号
    LastLogTerm int // 请求投票节点日志最大 term
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    // Your data here.
    Term int // 投票节点当前 term
    VoteGranted bool // 是否投票给申请节点
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here.
    // fmt.Printf("%v: Get request vote from %v, term %v\n", rf.me, args.CandidateId, args.Term)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    reply.VoteGranted = false
    // 如果申请投票节点 term 小于等于 当前节点，无法获得投票，并返回当前节点 term 用来更新申请投票节点 term
    if args.Term <= rf.currentTerm {
        reply.Term = rf.currentTerm
        //fmt.Printf("%v currentTerm:%v vote reject for:%v term:%v\n",rf.me,rf.currentTerm,args.CandidateId,args.Term)
        return
    }
    // 如果申请投票节点 term 大于 当前节点，则当前节点落后于申请投票节点，当前节点状态转移为追随者，更细当前节点状态
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = STATE_FLLOWER
        rf.voteFor = -1
    }
    reply.Term = rf.currentTerm

    // 保证申请投票节点所拥有的日志不少于当前节点，才能获得投票
    index := len(rf.log)
    term := rf.log[index - 1].LogTerm
    uptoDate := false
    if args.LastLogTerm > term {
        uptoDate = true
    }
    if args.LastLogTerm == term && args.LastLogIndex >= index { // at least up to date
        uptoDate = true
    }

    // 当前节点尚未投票，或已经投给此申请节点，并且前面条件都满足情况下，投票给此申请节点
    if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && uptoDate {
        rf.chanGrantVote <- true
        rf.state = STATE_FLLOWER
       reply.VoteGranted = true
        rf.voteFor = args.CandidateId
        // fmt.Printf("%v currentTerm:%v vote for:%v term:%v\n",rf.me,rf.currentTerm,args.CandidateId,args.Term)
    }
}

// 对所有其他节点申请投票
func (rf *Raft) boatcastRequestVote() {
    var args RequestVoteArgs
    rf.mu.Lock()
    args.LastLogIndex = len(rf.log)
    args.LastLogTerm = rf.log[len(rf.log) - 1].LogTerm
    args.Term = rf.currentTerm
    args.CandidateId = rf.me
    rf.mu.Unlock()

    for i := range rf.peers {
        if i != rf.me && rf.state == STATE_CANDIDATE {
            go func (i int) {
                var reply RequestVoteReply
                rf.sendRequestVote(i, args, &reply)
            }(i)
        }
    }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if ok {
        term := rf.currentTerm
        if rf.state != STATE_CANDIDATE {
            return ok
        }
        if args.Term != term {
            return ok
        }
        if reply.Term > term {
            rf.currentTerm = reply.Term
            rf.state = STATE_FLLOWER
            rf.voteFor = -1
            rf.persist()
        }
        if reply.VoteGranted {
            rf.voteCount++
            // 获得超过一半票，当选为领导者，向 chanLeader 通道发送消息
            if rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
                rf.state = STATE_FLLOWER
                rf.chanLeader <- true
            }
        }
    }
    return ok
}

type AppendEntriesArgs struct {
    Term int // 当前更新日志 term
    Leader int // 当前领导者 id
    PrevLogIndex int // 上一个未同步节点编号
    PrevLogTerm int // 上一个未同步节点 term
    LeaderCommit int // 领导者已提交日志最大编号

    Entries []LogEntry // 未同步日志
}

type AppendEntriesReply struct {
    PrevLogIndex int // 当前上一个未同步节点编号
    CurrentTerm int // 当前节点 term
    Success bool // 更新成功或失败
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    // 获得心跳包，重置超时计时器
    rf.chanHeartbeat <- true
    reply.CurrentTerm = rf.currentTerm

    // fmt.Printf("%v: currentTerm: %v, args.Term: %v\n", rf.me, rf.currentTerm, args.Term)
    // 当前节点 term 小于等于领导者节点，否则当前节点比领导者新，无法更新
    if rf.currentTerm <= args.Term {
        rf.currentTerm = args.Term
        rf.state = STATE_FLLOWER
        rf.voteFor = -1
        // 没有需要更新的日志
        if (args.PrevLogIndex >= len(rf.log)) {
            reply.PrevLogIndex = len(rf.log)
            reply.Success = false
            return
        }

        // fmt.Printf("log PrevLogIndex: %v %v\n", len(rf.log), args.PrevLogIndex)
        // 之前日志与领导者已经同步，可以更新之后的日志
        if args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex].LogTerm == args.PrevLogTerm {
            if len(args.Entries) > 0 {
                rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
                // fmt.Printf("%v: Add log: %v\n", rf.me, args.Entries)
            }
            if args.LeaderCommit > rf.commitIndex {
                last := len(rf.log)
                if args.LeaderCommit > last {
                    rf.commitIndex = last
                } else {
                    rf.commitIndex = args.LeaderCommit
                }
                rf.chanCommit <- true
            }
            reply.PrevLogIndex = len(rf.log)
            reply.Success = true
        // 之前日志与领导者不同步，需要向前回溯未更新日志，为防止一个一个回溯超时，每次回溯一个 term 日志
        } else {
            term := rf.log[args.PrevLogIndex].LogTerm
            for i := args.PrevLogIndex - 1; i >= 0; i-- {
                if (rf.log[i].LogTerm != term) {
                    reply.PrevLogIndex = i + 1
                    // fmt.Printf("last term begin: %v \n", reply.PrevLogIndex)
                    break
                }
            }
            // fmt.Printf("%v: PrevLogIndex: %v\n", rf.me, reply.PrevLogIndex)
            reply.Success = false
        }
    } else {
        reply.PrevLogIndex = 1
        reply.Success = false
    }

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    // fmt.Printf("%v: Send heartbeat to %v\n", rf.me, server)
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    // fmt.Printf("%v to %v: %v\n", rf.me, server, ok)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if ok {
        if rf.state != STATE_LEADER  || args.Term != rf.currentTerm {
            return ok
        }
    } else {
        return ok
    }

    if reply.Success {
        if len(args.Entries) > 0 {
            rf.nextIndex[server] = reply.PrevLogIndex
            rf.matchIndex[server] = rf.nextIndex[server] - 1
            // fmt.Printf("%v: nextIndex: %v\n", server, rf.nextIndex[server])
        }
    } else {
        // fmt.Printf("NextIndex: %v\n", rf.nextIndex[server])
        // 如果当前领导者落后于其他节点，领导者退为追随者
        if reply.CurrentTerm > rf.currentTerm {
            rf.state = STATE_FLLOWER
            rf.voteFor = -1
            rf.persist()
        // 更新失败，之前有未同步日志，回溯已经提交的日志
        } else {
            rf.nextIndex[server] = reply.PrevLogIndex
        }
    }

    return ok
}

func (rf *Raft) boatcastAppendEntries() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // 非领导者不需要发送消息，只需接受请求
    if rf.state == STATE_LEADER {
        // 检查之间的日志是否已经在半数以上节点储存，若是的话提交日志
        N := rf.commitIndex
        last := len(rf.log)
        for i := rf.commitIndex + 1; i < last; i++ {
            num := 1
            for j := range rf.peers {
                if j != rf.me && rf.matchIndex[j] >= i {
                    num++
                }
            }
            if 2*num > len(rf.peers) {
                N = i
            } else {
                break
            }
        }
        // 有新的未提交但可提交的日志，并且最新的可提交日志为当前 term 产生的日志，此处是为了防止论文中 Figure 8 情况发生
        if N != rf.commitIndex && rf.log[N].LogTerm == rf.currentTerm {
            rf.commitIndex = N
            rf.chanCommit <- true
        }
        // 对每个节点发送信息
        for i := range rf.peers {
            if i != rf.me && rf.state == STATE_LEADER {
                var args AppendEntriesArgs
                args.Term = rf.currentTerm
                args.Leader = rf.me
                args.LeaderCommit = rf.commitIndex
                args.PrevLogIndex = rf.nextIndex[i] - 1
                // fmt.Printf("%v: To %v, PrevLogIndex %v, log number %v, term %v\n", rf.me, i, args.PrevLogIndex, len(rf.log), rf.currentTerm)
                if args.PrevLogIndex >= 0 {
                    args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
                    args.Entries = make([]LogEntry, len(rf.log) - args.PrevLogIndex - 1)
                    copy(args.Entries, rf.log[args.PrevLogIndex + 1:])
                } else {
                    args.PrevLogTerm = 0
                    args.Entries = make([]LogEntry, len(rf.log))
                    copy(args.Entries, rf.log)
                }
                go func(i int, args AppendEntriesArgs) {
                    var reply AppendEntriesReply
                    rf.sendAppendEntries(i, args, &reply)
                }(i,args)
            }
        }
    }
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    index := -1
    term := rf.currentTerm

    isLeader := rf.state == STATE_LEADER

    if isLeader {
        index = len(rf.log)
        rf.log = append(rf.log, LogEntry{LogTerm: term, LogComd: command})
        // go rf.boatcastAppendEntries()
        rf.persist()
    }

    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here.
    rf.currentTerm = 0
    rf.voteFor = -1
    rf.state = STATE_FLLOWER
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.log = append(rf.log, LogEntry{LogTerm: 0})
    rf.chanCommit = make(chan bool,100)
    rf.chanHeartbeat = make(chan bool,100)
    rf.chanGrantVote = make(chan bool,100)
    rf.chanLeader = make(chan bool,100)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go func() {
        index := 1
        for {
            // fmt.Printf("%v: logs %v\n", rf.me, rf.log)
            index++
            switch rf.state {
            // 追随者需要在超市之前从领导者或者是被选举者处获得请求，否则认为领导者已经失去联系，开始选举，申请成为领导者
            case STATE_FLLOWER:
                select {
                case <- rf.chanHeartbeat:
                case <- rf.chanGrantVote:
                case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
                    rf.state = STATE_CANDIDATE
                }
            // 被选举者增加一个 term，从其他节点请求投票
            case STATE_CANDIDATE:
                // fmt.Printf("%v: Start candidate\n", rf.me)
                rf.mu.Lock()
                rf.currentTerm++
                rf.voteFor = me
                rf.voteCount = 1
                rf.mu.Unlock()
                // 请求投票
                go rf.boatcastRequestVote()
                select {
                case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
                    rf.currentTerm++
                case <-rf.chanHeartbeat:
                    // fmt.Printf("%v: Reveive chanHeartbeat\n",rf.me)
                    rf.state = STATE_FLLOWER
                case <- rf.chanLeader:
                    rf.mu.Lock()
                    rf.state = STATE_LEADER
                    // fmt.Printf("%v: Become leader\n",rf.me)
                    rf.nextIndex = make([]int, len(rf.peers))
                    rf.matchIndex = make([]int, len(rf.peers))
                    for i := range rf.peers {
                        rf.nextIndex[i] = len(rf.log)
                        rf.matchIndex[i] = 0
                    }
                    rf.mu.Unlock()
                    // rf.boatcastAppendEntries()
                }
            // 领导者发送消息，同步日志
            case STATE_LEADER:
                // fmt.Printf("%v: Send heartbeat %v\n", rf.me, index)
                rf.boatcastAppendEntries()
                time.Sleep(HBINTERVAL)
            }
        }
    }()
    // 提交可以提交的日志
    go func() {
        for {
            select {
            case <- rf.chanCommit:
                rf.mu.Lock()
                commitIndex := rf.commitIndex
                for i := rf.lastApplied + 1; i <= commitIndex; i++ {
                    // fmt.Printf("%v: Commit %v\n", rf.me, i)
                    msg := ApplyMsg{Index: i, Command: rf.log[i].LogComd}
                    applyCh <- msg
                }
                rf.lastApplied = commitIndex
                rf.mu.Unlock()
            }
        }
    }()

    return rf
}
