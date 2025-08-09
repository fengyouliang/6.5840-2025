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
	tester "6.5840/tester1"
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
	maxApplied  int

	// Volatile satte on leaders
	nextIndex  []int
	matchIndex []int
	// quorum     uint32

	// internal
	NodeState NodeState

	electionTimer *time.Timer

	applyCh chan raftapi.ApplyMsg
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

	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm { // implementation 1
		log.Printf("Term: %d Current: %d RequestVote from server %d, term < currentTerm  %#v, %#v", rf.currentTerm, rf.me, args.CandidateId, args, reply)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.NodeState = Follower
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // implementation 2
		// 检查候选者的日志是否至少和自己的日志一样新 (§5.2, §5.4)
		localLastLogIndex := len(rf.log)
		localLastLogTerm := -1
		if localLastLogIndex > 0 {
			localLastLogTerm = rf.log[localLastLogIndex-1].Term
		}

		// 比较日志的新旧程度
		if args.LastLogTerm < localLastLogTerm {
			// 候选者的最后日志条目任期小于本地，拒绝投票
			return
		}
		if args.LastLogTerm == localLastLogTerm && args.LastLogIndex < localLastLogIndex {
			// 任期相同但候选者的日志较短，拒绝投票
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// rf.resetElectionTimer()
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
	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Term: %d, Current: %d, recvice from %d args: %#v", rf.currentTerm, rf.me, args.LeaderId, args)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm { // implementation 1
		log.Printf("Term: %d, Current: %d, Leader %d's term %d < currentTerm %d. Reply false.", rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.NodeState = Follower

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(rf.log) {
			// 我们在prevLogIndex之前的日志条目比请求的少
			return
		}
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// 之前的日志条目不匹配
			return
		}
	}

	// 3. 处理日志冲突和追加新条目
	if len(args.Entries) > 0 {
		// 检查冲突：如果存在冲突，截断到PrevLogIndex之后
		conflict := false
		for i, entry := range args.Entries {
			localIndex := args.PrevLogIndex + 1 + i
			if localIndex <= len(rf.log) && localIndex > 0 {
				if rf.log[localIndex-1].Term != entry.Term {
					conflict = true
					break
				}
			}
		}

		// 截断到PrevLogIndex之后的位置
		if conflict || args.PrevLogIndex < len(rf.log) {
			if args.PrevLogIndex > 0 {
				rf.log = rf.log[:args.PrevLogIndex]
			} else if args.PrevLogIndex == 0 {
				rf.log = []logEntry{}
			}
		}

		// 追加所有新条目
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
		rf.persist()
	}

	// 5. 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if lastNewEntryIndex < args.LeaderCommit {
			rf.commitIndex = lastNewEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// 确保不提交未来任期的条目
		if rf.commitIndex > 0 && rf.commitIndex <= len(rf.log) {
			if rf.log[rf.commitIndex-1].Term <= rf.currentTerm {
				rf.applyCommittedEntries()
			}
		}
	}

	reply.Success = true
	rf.resetElectionTimer()
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	if rf.NodeState != Leader {
		return 0, term, false
	}

	index := len(rf.log) + 1
	rf.log = append(rf.log, logEntry{rf.currentTerm, command})
	rf.persist()

	// leader不应直接设置commitIndex - 应该由heartbeat机制根据follower的确认来设置
	// rf.commitIndex将由leader的AppendEntries RPC更新
	return index, term, true
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
			state := rf.NodeState
			rf.mu.Unlock()
			if state != Leader {
				go rf.startElection()
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.NodeState = Candidate // Conversion to candidate
	rf.currentTerm++         // Increment currentTerm
	rf.votedFor = rf.me      // Vote for self
	currentTerm := rf.currentTerm
	rf.resetElectionTimer() // Reset election timer

	log.Printf("Term: %d, Current: %d start election", rf.currentTerm, rf.me)
	rf.mu.Unlock()

	// 计算本地日志的最后索引和任期
	lastLogIndex := len(rf.log)
	lastLogTerm := -1
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	args := &RequestVoteArgs{currentTerm, rf.me, lastLogIndex, lastLogTerm}

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
				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.NodeState = Follower
				// rf.resetElectionTimer()
				return
			}

			if reply.VoteGranted && reply.Term == currentTerm {
				votes := atomic.AddInt32(&votes, 1)
				if votes >= majority && rf.NodeState == Candidate && rf.currentTerm == currentTerm {
					// If votes received from majority of servers: become leader
					rf.NodeState = Leader
					log.Printf("Term: %d, Current: %d become leader", rf.currentTerm, rf.me)

					// Initialize leader state
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}

					go rf.sendHeartbeat()
				}
			}
		}(i)
	}
	// 没有获取到大部分的投票，等待下一次的选举超时
	// If election timeout elapses: start new election
}

func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.NodeState != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntriesToPeer(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntriesToPeer(server int) {
	rf.mu.Lock()
	if rf.NodeState != Leader {
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := -1
	if prevLogIndex > 0 && prevLogIndex <= len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	} else if prevLogIndex == 0 {
		prevLogTerm = -1
	}

	entries := make([]logEntry, 0)
	if rf.nextIndex[server] <= len(rf.log) {
		entries = rf.log[rf.nextIndex[server]-1:]
	}

	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.NodeState != Leader || rf.currentTerm != currentTerm {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.NodeState = Follower
			return
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
			}
			rf.updateCommitIndex()
		} else {
			// 优化回退策略：如果nextIndex > 1，回退到冲突条目的开始
			if rf.nextIndex[server] > 1 {
				// 简单回退策略：先尝试直接回退到冲突位置
				if args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log) {
					// 找到冲突条目的任期，回退到该任期的第一个条目
					conflictTerm := rf.log[args.PrevLogIndex-1].Term
					newNextIndex := args.PrevLogIndex
					for newNextIndex > 1 && rf.log[newNextIndex-1-1].Term == conflictTerm {
						newNextIndex--
					}
					rf.nextIndex[server] = newNextIndex
				} else {
					// 简单递减
					rf.nextIndex[server]--
				}
			}
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	if len(rf.log) == 0 {
		return
	}

	// Update commitIndex based on matchIndex
	matchIndices := make([]int, len(rf.matchIndex))
	copy(matchIndices, rf.matchIndex)
	matchIndices[rf.me] = len(rf.log)

	// Sort matchIndices
	for i := 0; i < len(matchIndices); i++ {
		for j := i + 1; j < len(matchIndices); j++ {
			if matchIndices[i] > matchIndices[j] {
				matchIndices[i], matchIndices[j] = matchIndices[j], matchIndices[i]
			}
		}
	}

	// Find the N/2-th largest match index
	majorityIndex := matchIndices[len(matchIndices)/2]
	if majorityIndex > 0 && majorityIndex <= len(rf.log) {
		// 根据Raft规则：只提交当前任期的日志条目
		if rf.log[majorityIndex-1].Term == rf.currentTerm {
			if majorityIndex > rf.commitIndex {
				rf.commitIndex = majorityIndex
				rf.applyCommittedEntries()
			}
		}
	}
}

func (rf *Raft) applyCommittedEntries() {
	for rf.lastApplied < rf.commitIndex {
		if rf.lastApplied+1 > len(rf.log) {
			break
		}

		rf.lastApplied++
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
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
	rf.maxApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// rf.quorum = uint32(len(rf.peers) / 2)
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.dead = 0

	rf.NodeState = Follower
	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
