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
	"math/rand"
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

// 重置选举时间
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStarted = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)                     //定义了超时时间的范围
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange) //定义了超时时间
}

// 查看是否超时？
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStarted) > rf.electionTimeout
}

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
	role            Role
	currentTerm     int           //当前任期
	votedFor        int           //表示当前有没有投过票，投过谁。-1 means votes for none
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int
	CandidateId int //候选者的ID，供其他的peers选择是否投给它
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int  //其他peer返回的要票信息，返回自身的任期id
	VoteGranted bool //返回处理结果，标识是否投票给候选者
}

// example RequestVote RPC handler.
// 这里是RPC的接收方的处理信息,也就是Candidate向其他peer发起要票信息，其他peer的返回信息！
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	//上大锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//	1.对齐任期
	reply.Term = rf.currentTerm //返回结果的任期当然是当前节点的任期
	// 如果RPC的发起者(Candidate)的任期小于当前节点的任期，直接返回！拒绝投票，因为只会投票给高任期、高日志的节点！
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term) //变为跟随者
	}

	//2.检查该节点是否已经投过票了(单点单票机制)
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Cause Already Voted to S%d", args.CandidateId, rf.votedFor)
		return
	}

	//3.正式投票给RPC发起者
	reply.VoteGranted = true       //自己投过票了！
	rf.votedFor = args.CandidateId //投票给RPC的发起者(Candidate)
	rf.resetElectionTimerLocked()  //重置自己的选举时钟，这是来自(准)Leader的压迫
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote Granted", rf.role)

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

// 成为候选者后的投票细节
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		// Your code here (PartA)
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm) //向其他peers要票
		}
		rf.mu.Unlock()
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond) //注意睡眠时间也要随机
	}
}

// 上下文检测函数.
// 防止在RPC的过程中，如其他线程的并发操作等，导致自身的Candidate状态发生了变化，自己已经不是Candidate了，就直接终止！
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

// 日志RPC请求参数构造
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// 日志RPC返回参数构造
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// 接收方的心跳函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//	对齐任期
	if args.Term < rf.currentTerm {
		//发现RPC的接受者的任期大于自身的，直接返回
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	//来自高任期领导者的压制，强制你成为跟随者！
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	//重置选举时钟！
	rf.resetElectionTimerLocked()
}

// 发送方的心跳函数
func (rf *Raft) sendAppendEntries(sever int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[sever].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 心跳的逻辑(暂时不带任何日志)
// 这里的返回值bool是是否心跳成功！
func (rf *Raft) startReplication(term int) bool {
	//内存嵌套函数！
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		//	对齐任期
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//	检测是否发生上下文丢失
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader [%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go replicateToPeer(peer, args)
	}
	return true
}

// 固定间隔发起心跳检测！
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		time.Sleep(replicateInternal)
	}
}

// 选举的逻辑
func (rf *Raft) startElection(term int) {
	votes := 0
	//使用局部嵌套函数，方便进行votes++
	//这是第三个层次，也就是对一个peer发起RPC并处理返回值，看是否投票给自己
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)
		//	处理RPC结果
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DError, "Ask Vote from Peer S%d, Lost or error", peer)
			return
		}
		//	对齐任期
		if reply.Term > rf.currentTerm { //发现对方是高任期，说明对放在期间超时了，那直接自身变为跟随者！
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		//	检查上下文
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply for S%d", peer)
			return
		}
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
				//开始固定间隔并发进行日志复制处理
				go rf.replicationTicker(term)
			}
		}
	}

	//注意凡是这种不定时IO，都要不能加锁，因为担心死锁问题！
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost context, Candidate changes to %s, abort RequestVote", rf.role)
		return //如果检测到不是在这个term里面的Candidate，直接返回pass
	}
	for peer := 0; peer < len(rf.peers); peer++ { //挨个选举
		if peer == rf.me { //自己一定投票给自己
			votes++
			continue
		}
		args := &RequestVoteArgs{ //构造RPC的参数
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		go askVoteFromPeer(peer, args) //回调函数
	}

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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) //用于在系统崩溃恢复后从持久化状态中恢复数据。

	// start ticker goroutine to start elections
	go rf.electionTicker() //这里异步开启的一个选举线程，与整个Raft实例生命周期一致！

	return rf //这里返回这个指针
}
