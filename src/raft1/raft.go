package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int
	// 确保每个term都只有一个leader，防止当前term内投多个票
	// term增加时，应该同步修改votedFor

	log []logEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile satte on leaders
	nextIndex  []int
	matchIndex []int
	quorum     uint32

	// internal
	NodeState NodeState

	electionTimer *time.Timer
}

type logEntry struct {
	Term    int
	Command interface{}
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.NodeState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		log.Printf("Term: %d Current: %d RequestVote from server %d, term < currentTerm  %#v, %#v", rf.currentTerm, rf.me, args.CandidateId, args, reply)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.NodeState = Follower
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId

		reply.VoteGranted = true
		rf.resetElectionTimer()
		log.Printf("Term: %d, Current: %d RequestVote from server %d, votedFor %d", rf.currentTerm, rf.me, args.CandidateId, args.CandidateId)
	}
	return

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
	// rf.currentTerm 这里缺少了锁，所以还是不要print了
	// log.Printf("Term: %d, Current: %d sendRequestVote request to %d, %#v, %#v", rf.currentTerm, rf.me, server, args, reply)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// log.Printf("Term: %d, Current: %d sendRequestVote response from %d, %#v, %#v", rf.currentTerm, rf.me, server, args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Term: %d, Current: %d, recvice from %d args: %#v", rf.currentTerm, rf.me, args.LeaderId, args)

	reply.Term = rf.currentTerm
	reply.Success = false
	rf.resetElectionTimer()

	if args.Term < rf.currentTerm {
		log.Printf("Term: %d, Current: %d, Leader %d's term %d < currentTerm %d. Reply false.", rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.NodeState = Follower
	reply.Term = rf.currentTerm
	reply.Success = true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// todo: cleanup
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.NodeState != Leader {
				go rf.startElection()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.NodeState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	rf.resetElectionTimer()

	log.Printf("Term: %d, Current: %d start election", rf.currentTerm, rf.me)
	rf.mu.Unlock()

	args := &RequestVoteArgs{currentTerm, rf.me, -1, -1}

	votes := int32(1) // Vote for self
	majority := int32(len(rf.peers)/2 + 1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm != currentTerm || rf.NodeState != Candidate {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.NodeState = Follower
				rf.resetElectionTimer()
				return
			}

			if reply.VoteGranted && reply.Term == currentTerm {
				votes := atomic.AddInt32(&votes, 1)
				if votes >= majority && rf.NodeState == Candidate && rf.currentTerm == currentTerm {
					rf.NodeState = Leader
					log.Printf("Term: %d, Current: %d become leader", rf.currentTerm, rf.me)

					go rf.sendHeartbeat()
				}
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.NodeState != Leader {
			rf.mu.Unlock()
			return
		}

		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func() {
				args := &AppendEntriesArgs{currentTerm, rf.me, -1, -1, nil, -1}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, args, reply)

				if ok && reply.Term > currentTerm {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.NodeState = Follower
						rf.resetElectionTimer()
					}
					rf.mu.Unlock()
				}
			}()
		}

		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.getElectionTimeout())
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(400)) * time.Millisecond
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.quorum = uint32(len(rf.peers) / 2)
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}
	rf.dead = 0

	rf.NodeState = Follower
	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
