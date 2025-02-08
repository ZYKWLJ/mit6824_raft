package raft

import (
	"math/rand"
	"time"
)

// 重置选举时间
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStarted = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)                     //定义了超时时间的范围
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange) //定义了超时时间
}

// 比较候选者与RPC接收者最后一个日志谁的新？
// 看看是不是RPC接收者的日志更新
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	l := len(rf.log)
	lastIndex, lastTerm := l-1, rf.log[l-1].Term
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	//两者任期不相等的话
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	//	两者任期相等的话，比较最后日志新旧
	return lastIndex > candidateIndex
}

// 查看是否超时？
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStarted) > rf.electionTimeout
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term         int
	CandidateId  int //候选者的ID，供其他的peers选择是否投给它
	LastLogIndex int //日志的最后的下标和任期
	LastLogTerm  int
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
	//2.1这里就是检测的候选者和RPC接收者谁的最后日志最新！
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		//Reject the log election cause the RPC's receiver's last LOG is more updated
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Candidate less up to date", args.CandidateId)
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
	l := len(rf.log)
	for peer := 0; peer < len(rf.peers); peer++ { //挨个选举
		if peer == rf.me { //自己一定投票给自己
			votes++
			continue
		}
		args := &RequestVoteArgs{ //构造RPC的参数
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log[l-1].Term,
		}
		go askVoteFromPeer(peer, args) //回调函数
	}

}
