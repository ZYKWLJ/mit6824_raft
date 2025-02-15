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
	//	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
	//PartA细节1: 日志复制间隔要比选举的小，避免日志复制失败角色就变了！
	replicateInterval time.Duration = 70 * time.Millisecond
)

const (
	//PartA细节2: 头日志定义(类似于头结点),做一致性处理
	InvalidTerm  int = 0
	InvalidIndex int = 0
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool        //用于指示该日志条目中的命令是否有效。
	Command      interface{} //存储任何类型的数据
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]每一个节点在节点集群中的下标！
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int
	votedFor    int // -1 means vote for none

	// log in the Peer's local
	log *RaftLog

	// only used in Leader
	// every peer's view
	//PartA细节3 :
	//这代表Leader 需要维护一个各个 Peer 的进度视图
	//(也就是已匹配点、下一个待匹配点)
	//主节点的第一件事就是对齐nextIndex，然后做一致性检查(复制)！
	//这里存储着每一个节点的nextIndex、matchIndex
	//Leader 正是依据此视图来决定给各个 Peer 发送多少日志。也是依据此视图，Leader 可以计算全局的 commitIndex。
	
	nextIndex  []int
	matchIndex []int

	// fields for apply loop
	commitIndex int //每个 Follower 收到 commitIndex 之后，再去 apply 本地的已提交日志到状态机。
	lastApplied int
	applyCh     chan ApplyMsg
	snapPending bool
	applyCond   *sync.Cond

	electionStart   time.Time
	electionTimeout time.Duration // random
}

//PartA:三者角色转换的统一步骤、
//1.判断能不能成？
//2.成了干什么？

func (rf *Raft) becomeFollowerLocked(term int) {
	//PartA:细节2.如果RPC接收方的任期大于发起方的任期，那么直接拒绝投票，表示不能成为Leader！
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%v->T%v", rf.role, rf.currentTerm, term)
	rf.role = Follower
	//PartA细节4: 任期相等的逻辑处理，防止宕机重启后前面任期的数据丢失，至于当前任期，直接可以同步的！
	shouldPersit := rf.currentTerm != term
	//PartA细节5: 重要，仅仅任期升高才会重置投票~
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	//任期升高(向主节点收敛)
	rf.currentTerm = term
	//细节4讲的持久化
	if shouldPersit {
		rf.persistLocked()
	}
}

// PartA 细节6:F->C 唯一条件——> 任期超时，发起选举~
func (rf *Raft) becomeCandidateLocked() {
	//PartA 细节7:没有L->C的角色流向
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	//PartA 细节8:称为Candidate后的动作

	//8-1.重置选举时钟，以期待下一次选举成为L！
	rf.resetElectionTimerLocked()
	//8-2.触发条件就是任期超时，当然增加任期
	rf.currentTerm++
	//8-3.角色转换
	rf.role = Candidate
	//8-4.一定向自己投一票
	rf.votedFor = rf.me
	//8-5.一定会持久化，因为持久化三大件(日志、任期、投票)发生了变化!
	rf.persistLocked()
}

// PartA 细节9:唯一条件——>候选者多票任选(就像体制内逐步升迁一样)
func (rf *Raft) becomeLeaderLocked() {
	//不是候选者，直接嘎(就是针对总结的话进行判断而已！)
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
	//PartA 细节10:称为Leader后的动作
	//称为L后的所有工作：所有节点向L收敛
	//收敛前的准备动作：
	//第一件事就是对齐`已匹配下标(0是一定匹配的，因为头日志的存在)、待匹配下标(从自身的日志最后开始算起)`
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)
	rf.persistLocked()

	return rf.log.size() - 1, rf.currentTerm, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 上下文是否丢失采用任期和角色是否相等来判断的
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
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
	fmt.Printf("新建一个Raft实例%d\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// a dummy entry to aovid lots of corner checks
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)

	// initialize the leader's view slice
	//正式初始化后再成功当选为L后在开始
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize the fields used for apply
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapPending = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()

	return rf
}
