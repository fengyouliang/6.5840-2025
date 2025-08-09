package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// --------------------------------------------------
//  基础结构
// --------------------------------------------------

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

// Raft 节点
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	// ---------- 持久化 ----------
	currentTerm int
	votedFor    int
	log         []logEntry

	// ---------- 易失 ----------
	commitIndex int
	lastApplied int

	// ---------- Leader 易失 ----------
	nextIndex  []int
	matchIndex []int

	// ---------- 内部 ----------
	state         NodeState
	electionTimer *time.Timer
	applyCh       chan raftapi.ApplyMsg
}

// --------------------------------------------------
//  锁内工具函数
// --------------------------------------------------

func (rf *Raft) becomeFollowerLocked(term int) {
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.state = Follower
	rf.resetElectionTimerLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader
	last := len(rf.log) + 1
	for i := range rf.peers {
		rf.nextIndex[i] = last
		rf.matchIndex[i] = 0
	}
	DPrintf("S%d become leader at term %d", rf.me, rf.currentTerm)
	go rf.leaderLoop()
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionTimer.Reset(time.Duration(400+rand.Intn(400)) * time.Millisecond)
}

// --------------------------------------------------
//  RPC 结构
// --------------------------------------------------

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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

// --------------------------------------------------
//  RPC 处理
// --------------------------------------------------

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. term 太小
	if args.Term < rf.currentTerm {
		return
	}
	// 2. 发现更大 term，立即退为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 再次写入最新 term
	reply.Term = rf.currentTerm

	// 3. 已投过且不是同一候选人
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// 4. 日志是否至少一样新
	lastIdx, lastTerm := rf.lastLogInfo()
	upToDate := (args.LastLogTerm > lastTerm) ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
	if !upToDate {
		return
	}

	// 5. 投票
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	reply.VoteGranted = true
	rf.resetElectionTimerLocked()
	DPrintf("S%d voted for S%d at term %d", rf.me, args.CandidateId, rf.currentTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. term 太小
	if args.Term < rf.currentTerm {
		return
	}
	// 发现更大 term
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	rf.state = Follower
	rf.resetElectionTimerLocked()

	// 2. PrevLog 检查
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(rf.log) {
			return
		}
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return
		}
	}

	// 3. 处理冲突并追加
	idx := args.PrevLogIndex
	for i, ent := range args.Entries {
		pos := idx + 1 + i
		if pos <= len(rf.log) {
			if rf.log[pos-1].Term != ent.Term {
				rf.log = rf.log[:pos-1] // 截断
			} else {
				continue // 已存在且匹配
			}
		}
		rf.log = append(rf.log, ent)
	}
	rf.persistLocked()

	// 4. 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit > len(rf.log) {
			newCommit = len(rf.log)
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCommittedLocked()
		}
	}
	reply.Success = true
}

// --------------------------------------------------
//  日志复制（Leader）
// --------------------------------------------------

func (rf *Raft) sendAppendEntriesToPeer(server int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		prevIdx := rf.nextIndex[server] - 1
		prevTerm := -1
		if prevIdx > 0 {
			prevTerm = rf.log[prevIdx-1].Term
		}
		entries := make([]logEntry, len(rf.log)-prevIdx)
		copy(entries, rf.log[prevIdx:])
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.matchIndex[server] = newMatch
			rf.nextIndex[server] = newMatch + 1
			rf.updateCommitLocked()
			rf.mu.Unlock()
			return
		} else {
			// 快速回退：按任期回退
			if args.PrevLogIndex > 0 {
				conflictTerm := rf.log[args.PrevLogIndex-1].Term
				newNext := args.PrevLogIndex
				for newNext > 1 && rf.log[newNext-2].Term == conflictTerm {
					newNext--
				}
				rf.nextIndex[server] = newNext
			} else {
				rf.nextIndex[server] = 1
			}
		}
		rf.mu.Unlock()
	}
}

// --------------------------------------------------
//  Leader 主循环
// --------------------------------------------------

func (rf *Raft) leaderLoop() {
	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
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

// --------------------------------------------------
//  选举
// --------------------------------------------------

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persistLocked()
	currentTerm := rf.currentTerm
	lastIdx, lastTerm := rf.lastLogInfo()
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
	rf.resetElectionTimerLocked()
	DPrintf("S%d start election at term %d", rf.me, currentTerm)
	rf.mu.Unlock()

	votes := int32(1)
	majority := int32(len(rf.peers)/2 + 1)
	var voteMu sync.Mutex // 保护 votes

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != currentTerm || rf.state != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}
			if reply.VoteGranted && reply.Term == currentTerm {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
				if atomic.LoadInt32(&votes) >= majority {
					rf.becomeLeaderLocked()
				}
			}
		}(i)
	}
}

// --------------------------------------------------
//  提交 & apply
// --------------------------------------------------

func (rf *Raft) updateCommitLocked() {
	if len(rf.log) == 0 {
		return
	}
	// 复制一份并排序
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	tmp[rf.me] = len(rf.log)
	// 冒泡排序
	for i := 0; i < len(tmp)-1; i++ {
		for j := 0; j < len(tmp)-i-1; j++ {
			if tmp[j] > tmp[j+1] {
				tmp[j], tmp[j+1] = tmp[j+1], tmp[j]
			}
		}
	}
	n := tmp[len(tmp)/2]
	if n > rf.commitIndex && n <= len(rf.log) && rf.log[n-1].Term == rf.currentTerm {
		rf.commitIndex = n
		rf.applyCommittedLocked()
	}
}

func (rf *Raft) applyCommittedLocked() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}

// --------------------------------------------------
//  持久化（占位，3C 再实现）
// --------------------------------------------------

func (rf *Raft) persistLocked() {
	// TODO: 实现
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	// TODO: 实现
}

// --------------------------------------------------
//  辅助
// --------------------------------------------------

func (rf *Raft) lastLogInfo() (index int, term int) {
	index = len(rf.log)
	if index > 0 {
		term = rf.log[index-1].Term
	} else {
		term = -1
	}
	return
}

// --------------------------------------------------
//  对外接口
// --------------------------------------------------

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return 0, rf.currentTerm, false
	}
	index := len(rf.log) + 1
	rf.log = append(rf.log, logEntry{rf.currentTerm, command})
	rf.persistLocked()
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// --------------------------------------------------
//  后台循环
// --------------------------------------------------

func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			go rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

// --------------------------------------------------
//  Make
// --------------------------------------------------

// Snapshot 占位实现（3D 作业时再填）
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO: 3D 实现 snapshot
}

// PersistBytes 返回持久化数据大小
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		applyCh:       applyCh,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]logEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		state:         Follower,
		electionTimer: time.NewTimer(time.Duration(400+rand.Intn(400)) * time.Millisecond),
	}
	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	return rf
}
