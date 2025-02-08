package raft

//这里是放的所有的共有逻辑！其他分支逻辑，比如选举、复制等就单独分离出去！
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
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

// 选举超时时间上下界
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
	replicateInternal  time.Duration = 200 * time.Millisecond //日志复制的时间间隔一定要比选举下界小，因为这样才能保证完成一次日志复制
)

// define three roles-Leader 、follower、Candidate
type Role string

const (
	Leader    Role = "Leader"
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
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
	CommandValid bool
	Command      interface{}
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
	me        int                 // this peer's index into peers[]这是peer集群下的编号
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int //当前任期
	votedFor    int //表示当前有没有投过票，投过谁。-1 means votes for none
	//log in the Peer's local
	log []LogEntry

	//the following two fields only used in Leader
	//the two equivalent to every peer's view
	//这两个属性相当于每一个实例的视图
	nextIndex  []int
	matchIndex []int

	//日志运用的属性(需要make时进行初始化)
	//当commit更新并且大于lastApply时，就触发日志的apply
	commitIndex     int
	lastApplied     int
	applyCh         chan ApplyMsg
	applyCond       *sync.Cond    //golang 的条件变量(返回的是指针)
	electionStarted time.Time     //选举开始时间
	electionTimeout time.Duration //选举的间隔(真实判断再次发起选举的条件就是当前的时间减去开始的时间然后查看有没有大于时间间隔)
}

// 变为跟随者
// 从其他的Raft实例获得的term信息，这里是使得其他的实例变为当前传入term的follower！
// 这里是Raft类上绑定的方法，不是函数！
func (rf *Raft) becomeFollowerLocked(term int) {
	//如果传入的term小于自身类的term，则自身不会成为follower，因为followe的Term永远不会是最大的！
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "cannot become Follower, lower term: T%d", term)
		return
	}
	//LOG展示任期的变化（变为Follower）:收到了来自其他实例的高任期心跳(政令)，高任期实例强制压迫其他实例成为Follower，任期向Leader收敛！
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%s->T%s", rf.role, rf.currentTerm, term)
	rf.role = Follower         //被压迫了，强制成为Follower
	if term > rf.currentTerm { //迈入新的term时，会重置投票(因为外面是==的逻辑！这里新增的>的逻辑！)
		rf.votedFor = -1
	}
	rf.currentTerm = term //低任期向高任期实例的任期收敛
}

// 变为候选者条件
func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader cannot become Candidate")
		return
	}
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.currentTerm++              //变为候选者，任期会增加(是选举时间过期了，所以任期增加~)
	rf.role = Candidate           //变为候选者
	rf.votedFor = rf.me           //一定会给自己投一票
	rf.resetElectionTimerLocked() //重置选举时钟
}

// 变为领导者的条件
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Cannot become Leader, not Candidate")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "become Leader in T%d", rf.currentTerm)
	rf.role = Leader
	//initialization the peers matchIndex
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0 //The Dummy log always Equaled
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (PartA).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
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
	// Your code here (PartC).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// 这是raft对外提供的接口！外面使用raft来完成一致性任务的！
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

	// Your code here (PartB).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 上下文检测函数.
// 防止在RPC的过程中，如其他线程的并发操作等，导致自身的Candidate状态发生了变化，自己已经不是Candidate了，就直接终止！
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

// 这是节点初始化的函数！返回的是raft节点的指针
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{} //rf是一个Raft的指针
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1 //-1 means votes for none cause this is initialization
	//the Dummy entry to avoid lots of corner checks which like The Dummy Node in the list!
	rf.log = append(rf.log, LogEntry{})
	//initialize the leader's view slice
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) //用于在系统崩溃恢复后从持久化状态中恢复数据。
	//initialize The field used to apply
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu) //上一把大锁！

	// start ticker goroutine to start elections
	go rf.electionTicker() //这里异步开启的一个选举线程，与整个Raft实例生命周期一致！
	go rf.applicationTicker()
	return rf //这里返回这个指针
}
