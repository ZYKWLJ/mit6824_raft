package raft

import (
	"sort"
	"time"
)

// this is log entry
// 日志结构体的信息也是很多的！
type LogEntry struct {
	Term         int         //日志条目的任期
	CommandValid bool        //是不是应该应用到状态机？
	Command      interface{} //raft是运用到多机上一致的操作日志，这里的Command就是这个操作日志。这里的Command类型是任意的结构体！
	//CommandIndex int         //日志条目的提交位置(就是偏移offset)
}

// 日志RPC请求参数构造
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// the following two fields which used to probe the match point can Uniquely determine the log .
	// cause in a term,there is only one leader can replicate logs and each log has the unique index!
	PrevLogIndex int
	PrevLogTerm  int
	//after the above matchIndex ,all the logs should append to each follower. the following is all the logs!
	Entries []LogEntry
	//used to update the follower's commitIndex
	LeaderCommit int
}

// 日志RPC返回参数构造
type AppendEntriesReply struct {
	Term    int
	Success bool
	//PartC——实现日志回溯的精确定位，通过告知Leader自己的日志到哪里，直接方便Leader同步剩下的日志！
	//第一条做法的目的在于，如果Follower日志过短，可以提示Leader迅速回退到Follower日志的末尾，而不用傻傻 的一个个index或者term往前试探。
	ConflictIndex int
	//第二条的目的在于，如果Follower存在Leader.PrevLog,但不匹配，则将对应term的日志全部跳过。
	ConflictTerm int
}

// 接收方的心跳函数(包括日志复制！)//这里主要是针对Followers
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false //except all the false situations,the remain one is correct
	//	对齐任期(任何RPC都是这样的，所以这也体现了任期的绝对领导性！)
	if args.Term < rf.currentTerm {
		//发现RPC的接受者的任期大于自身的，直接返回
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	//来自高任期领导者的压制，强制你成为跟随者！
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	//立马重置选举时钟！防止日志复制时长很长，导致选举错过！
	//defer 将函数调用延迟到包含 defer 语句的函数即将返回时执行，常用于资源清理、解锁等操作，以确保这些操作在函数结束时一定会被执行。
	defer rf.resetElectionTimerLocked()
	//return false if prevLog not matched
	//表示领导者希望追随者从哪个日志索引开始追加新的日志条目。也就是说，领导者期望追随者的日志中在 args.PrevLogIndex 这个位置已经有一条相同的日志。
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictTerm = InvalidTerm
		reply.ConflictIndex = len(rf.log)
		//意味着领导者要求追随者从一个超出其当前日志长度的索引位置开始追加日志。这是不合理的，因为追随者的日志中根本不存在 args.PrevLogIndex 这个位置的日志，说明领导者和追随者的日志状态不一致，追随者无法按照领导者的要求进行日志复制。
		//LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:T%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	//如果对齐的日志任期不相等，直接FALSE！
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.firstLogFor(reply.ConflictTerm) //冲突的第一条日志
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d!=T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	//Append the leader's log entries to the local
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked() //must persist，cause it has changed term\voteFor\log
	//直到这里才是成功日志对齐！
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs:(%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries)) //the ranges of the follower has accepted
	//TODO(handle the leaderCommit)
	//这里是跟随者的日志提交
	//如果发送方的已提交下标大于接收方的已提交下标，直接开始弥补follower的提交
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

}

// 发送方的心跳函数
func (rf *Raft) sendAppendEntries(sever int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[sever].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 这样我们的Leader在成功收到Append的请求之后，我们就会更新matchIndex，进而更新commitIndex
func (rf *Raft) getMajorityIndexLocked() int {
	//切片是一种动态数组，使用起来比普通数组更加灵活，make 函数专门用于创建切片、映射和通道等引用类型。
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority [%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

// 心跳的逻辑(暂时不带任何日志)这里主要是针对Leader！
// 这里的返回值bool是是否心跳成功！
func (rf *Raft) startReplication(term int) bool {
	//内存嵌套函数！
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		//先检测是否能成功发送RPC
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
		//	处理回复handle the reply
		//先检测日志匹配不成功的情况，如果不匹配就探测更低的匹配点，到最终的开始一定是匹配的，因为头结点的存在！
		if !reply.Success {
			//日志回溯的版本2，接受reply的匹配日志信息，快速定位
			preIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm { //一直匹配到最低点(头结点)
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstIndex := rf.firstLogFor(reply.ConflictTerm)
				if firstIndex != reply.ConflictIndex {
					rf.nextIndex[peer] = firstIndex
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			//avoid unordered reply
			//匹配探测期比较长时，会有多个探测的 RPC，如果 RPC 结果乱序回来：一个先发出去的探测 RPC 后回来了，
			//其中所携带的 ConflictTerm 和 ConflictIndex 就有可能造成 rf.next 的“反复横跳”。为此，我们可以强制 rf.next 单调递减：
			if rf.nextIndex[peer] > preIndex {
				rf.nextIndex[peer] = preIndex
			}
			//这是日志回溯的，版本1已经废除，太费时间了！

			////先实现次优化的，后面再优化性能。先关注功能，再关注性能
			////go back term
			////这里的逻辑是怎么样的？
			//idx, term := args.PrevLogIndex, args.PrevLogTerm
			////这里的每次回退都是一个RPC，那么回退次数过多的话会造成RPC延迟过长，不太好！
			//for idx > 0 && rf.log[idx].Term == term { //上一个任期一定是相匹配的！
			//	idx--
			//}
			//rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Not match with S%d in %d, try next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}
		//如果日志追加成功，更新已匹配点和下一个欲匹配下标！
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// TODO:update the commitIndex
		// 这里是主节点的日志提交
		// Leader在收到Follower成功Append后，便更新已提交的下标(commitIndex)，指导follower的本地apply=>Signal()循环！
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex {
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal() //唤醒之前的wait()
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
			//self matchIndex handler
			//自身匹配点肯定是日志长度减1
			//自身下一个欲匹配点是日志长度！
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term
		//发送构造参数！
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.currentTerm,
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
