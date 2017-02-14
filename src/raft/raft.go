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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "encoding/gob"


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
    Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
    LogTerm int
    LogComd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex
    peers     []*labrpc.ClientEnd
    persister *Persister
    me        int // index into peers[]

    // Your data here.
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    currentTerm int
    voteFor int
    voteCount int
    state int
    chanCommit chan bool
    chanHeartbeat chan bool
    chanGrantVote chan bool
    chanLeader chan bool
    chanApply chan ApplyMsg

    log []LogEntry

    commitIndex int
    lastApplied int

    nextIndex []int
    matchIndex []int
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
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    // Your data here.
    LogNumber int
    Term int
    LeaderId int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    // Your data here.
    Term int
    Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here.
    fmt.Printf("%v: Get request vote from %v, term %v\n", rf.me, args.LeaderId, args.Term)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if (args.Term > rf.currentTerm && rf.commitIndex <= args.LogNumber) || ((args.Term == rf.currentTerm && rf.commitIndex <= args.LogNumber) && rf.voteFor == -1) {
        rf.currentTerm = args.Term
        rf.voteFor = args.LeaderId

        reply.Term = args.Term
        reply.Success = true
        fmt.Printf("%v: Vote to %v\n", rf.me, args.LeaderId)
    } else {
        fmt.Printf("%v: Don't vote to %v\n", rf.me, args.LeaderId)
        reply.Term = rf.currentTerm
        reply.Success = false
    }
}

func (rf *Raft) boatcastRequestVote() {
    var args RequestVoteArgs
    rf.mu.Lock()
    args.LogNumber = rf.commitIndex
    args.Term = rf.currentTerm
    args.LeaderId = rf.me
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
        if reply.Success {
            rf.voteCount++
            if rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
                rf.state = STATE_FLLOWER
                rf.chanLeader <- true
            }
        }
    }
    return ok
}

type AppendEntriesArgs struct {
    Term int
    Leader int
    PrevLogIndex int
    PrevLogTerm int
    LeaderCommit int

    Entries []LogEntry
}

type AppendEntriesReply struct {
    PrevLogIndex int
    CurrentTerm int
    Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.chanHeartbeat <- true
    reply.CurrentTerm = rf.currentTerm

    fmt.Printf("%v: currentTerm: %v, args.Term: %v\n", rf.me, rf.currentTerm, args.Term)
    if rf.currentTerm <= args.Term {
        rf.currentTerm = args.Term
        rf.state = STATE_FLLOWER
        rf.voteFor = -1
        if args.PrevLogIndex == 0 || (args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].LogTerm == args.PrevLogTerm) {
            if len(args.Entries) > 0 {
                rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
                fmt.Printf("%v: Add log: %v\n", rf.me, args.Entries)
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
        } else {
            reply.PrevLogIndex = args.PrevLogIndex - 1
            fmt.Printf("PrevLogIndex: %v\n", args.PrevLogIndex)
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
    fmt.Printf("%v to %v: %v\n", rf.me, server, ok)

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
            fmt.Printf("%v: nextIndex: %v\n", server, rf.nextIndex[server])
        }
    } else {
        fmt.Printf("NextIndex: %v\n", rf.nextIndex[server])
        if reply.CurrentTerm > rf.currentTerm {
            rf.state = STATE_FLLOWER
            rf.voteFor = -1
        }
        rf.nextIndex[server] = reply.PrevLogIndex
    }

    return ok
}

func (rf *Raft) boatcastAppendEntries() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state == STATE_LEADER {

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
        fmt.Printf("---------------------------------%v\n", N)
        if N != rf.commitIndex {
            rf.commitIndex = N
            rf.chanCommit <- true
        }

        for i := range rf.peers {
            if i != rf.me && rf.state == STATE_LEADER {
                var args AppendEntriesArgs
                args.Term = rf.currentTerm
                args.Leader = rf.me
                args.LeaderCommit = rf.commitIndex
                args.PrevLogIndex = rf.nextIndex[i] - 1
                fmt.Printf("%v: To %v, PrevLogIndex %v, log number %v, term %v\n", rf.me, i, args.PrevLogIndex, len(rf.log), rf.currentTerm)
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
        fmt.Printf("%v: Command %v\n", rf.me, command)
        index = len(rf.log)
        rf.log = append(rf.log, LogEntry{LogTerm: term, LogComd: command})
        // go rf.boatcastAppendEntries()
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
            fmt.Printf("%v: logs %v\n", rf.me, rf.log)
            index++
            switch rf.state {
            case STATE_FLLOWER:
                select {
                case <- rf.chanHeartbeat:
                case <- rf.chanGrantVote:
                case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
                    rf.state = STATE_CANDIDATE
                }
            case STATE_CANDIDATE:
                fmt.Printf("%v: Start candidate\n", rf.me)
                rf.mu.Lock()
                rf.currentTerm++
                rf.voteFor = me
                rf.voteCount = 1
                rf.mu.Unlock()
                go rf.boatcastRequestVote()
                select {
                case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
                case <-rf.chanHeartbeat:
                    fmt.Printf("%v: Reveive chanHeartbeat\n",rf.me)
                    rf.state = STATE_FLLOWER
                case <- rf.chanLeader:
                    rf.mu.Lock()
                    rf.state = STATE_LEADER
                    fmt.Printf("%v: Become leader\n",rf.me)
                    rf.nextIndex = make([]int, len(rf.peers))
                    rf.matchIndex = make([]int, len(rf.peers))
                    for i := range rf.peers {
                        rf.nextIndex[i] = len(rf.log)
                        rf.matchIndex[i] = 0
                    }
                    rf.mu.Unlock()
                    // rf.boatcastAppendEntries()
                }
            case STATE_LEADER:
                // fmt.Printf("%v: Send heartbeat %v\n", rf.me, index)
                rf.boatcastAppendEntries()
                time.Sleep(HBINTERVAL)
            }
        }
    }()

    go func() {
        for {
            select {
            case <- rf.chanCommit:
                rf.mu.Lock()
                commitIndex := rf.commitIndex
                for i := rf.lastApplied + 1; i <= commitIndex; i++ {
                    fmt.Printf("%v: Commit %v\n", rf.me, i)
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
