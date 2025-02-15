package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Term         int         // the log entry's term
	CommandValid bool        // if it should be applied 指示该日志条目中的命令是否有效。
	Command      interface{} // the command should be applied to the state machine 存储的数据
}

// PartA 日志追加 细节1 Leader的日志追加的发送RPC参数
type AppendEntriesArgs struct {
	//日志复制就是对比这几个参数，所以传递这几个参数，厘清这几个参数的相互影响即可~~
	Term     int
	LeaderId int

	// used to probe the match point
	//标识已匹配的下标
	PrevLogIndex int
	PrevLogTerm  int
	//表示匹配后需要追加的日志！
	Entries []LogEntry

	// used to update the follower's commitIndex
	//表示从节点应该更新自己的下标了！
	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

// 从节点的追加日志的返回值
type AppendEntriesReply struct {
	Term    int
	Success bool //表示跟随者是否成功接收并追加了领导者发送的日志条目。
	//追加不成功的标记
	ConfilictIndex int //当 Success 为 false 时，该字段表示跟随者与领导者日志出现冲突的第一个日志条目的索引。没有冲突为0。
	ConfilictTerm  int //当 Success 为 false 时，该字段表示冲突日志条目所在的任期号。
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// Peer's callback 回调函数
// PartA 日志复制 细节2:RPC接收方的函数！
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())
	//起始保守赋值
	reply.Term = rf.currentTerm
	reply.Success = false

	// align the term
	//第一件事就是对齐任期~
	//发送发任期小于接收方任期，直接返回，不可能接受这个日志
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	//表示受到了高任期的压制~被迫成为了跟随者
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() {
		//必须重置选举时钟，表示向高任期臣服！
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, rf.log.String())
		}
	}()
	// PartA 日志复制 细节3: 复制失败的集中情况
	//失败1，已匹配下标越界：超长
	// return failure if prevLog not matched
	// 如果传入的匹配下标大于日志长度了，绝对错误的。保守返回。
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictTerm = InvalidTerm
		reply.ConfilictIndex = rf.log.size()
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	//失败2，已匹配下标越界：超短
	//如果传入的匹配下标小于快照起始下标，表示这是已经被截断的日志，也直接返回！
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConfilictTerm = rf.log.snapLastTerm
		reply.ConfilictIndex = rf.log.snapLastIdx
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIdx)
		return
	}
	//失败3，已匹配下标在范围内但任期不匹配
	//如果对应下标的任期不相等，则将返回参数的冲突任期定为已匹配点的任期，冲突下标定为该任期下的第一条日志，返回
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}
	// 上面三种都通过，则开始将Leader的日志追加到Follower
	// append the leader log entries to local
	//本函数代表从某个下标开始追加
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	//Follower节点持久化
	rf.persistLocked()
	//返回已匹配成功~
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	//如果Leader的已提交下标大于Follower节点的，则将的提交下标更新为Leader的，再唤醒后台线程应用于应用层，即apply日志
	// hanle LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

}

// RPC的发送方函数
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

// PartA 心跳逻辑:细节3 开启心跳(复制)
// only valid in the given `term`
// 这是主节点Leader的逻辑
func (rf *Raft) startReplication(term int) bool {
	//PartA 心跳逻辑:细节4 心跳(日志复制)RPC内置函数
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		//构造RPC返回值
		reply := &AppendEntriesReply{}
		//查看是否发送RPC成功
		ok := rf.sendAppendEntries(peer, args, reply) //会通过RPC调用日志复制的逻辑，即使AppendEntries函数！

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// align the term
		//第一件事：对齐任期
		//如果RPC接受者任期大于发送方，直接变为F
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		//检测上下文丢失否？
		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		// hanle the reply
		// probe the lower index if the prevLog not matched
		// 细节前移指针直至匹配日志
		// PratA 疑问点1:为什么不直接rf.nextIndex[peer] = reply.ConfilictIndex？而需要再任期判等时在进行更新
		// 表明领导者与跟随者之间的日志存在不一致，此时领导者需要调整发送给该跟随者的下一个日志索引 nextIndex，以尝试重新同步日志。
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]         //记录了领导者当前认为应该发送给 peer 跟随者的下一条日志的索引，以便后续可能的分析或使用
			if reply.ConfilictTerm == InvalidTerm { //冲突的日志任期为0
				//出现冲突的第一个日志下标
				rf.nextIndex[peer] = reply.ConfilictIndex //异常任期的日志冲突比较特殊，直接从冲突位置开始重新同步日志是一种简单有效的策略。
			} else { //不为0
				//当冲突日志的任期是有效的正常值时，领导者首先尝试查找自己日志中与冲突任期相同的第一条日志的全局索引 firstIndex（通过 rf.log.firstFor(reply.ConfilictTerm)
				firstIndex := rf.log.firstFor(reply.ConfilictTerm)

				if firstIndex != InvalidIndex {
					//对齐下标
					rf.nextIndex[peer] = firstIndex
				} else {
					//若 firstIndex 为无效索引，表明领导者的日志中`不存在与跟随者冲突日志相同任期的部分`。说明跟随者的日志与领导者的日志差异较大。
					//将 nextIndex 设置为 ConfilictIndex 可以让领导者从冲突发生的位置开始重新尝试发送日志，从而缩小了搜索范围，避免了不必要的日志发送，提高了日志复制的效率。
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			//这里是确保Term一定是递增的，避免网络故障导致的乱序问题
			// avoid unordered reply
			// avoid the late reply move the nextIndex forward again
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIdx {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}
		// 这里就是匹配成功了后的流程
		// update match/next index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // important
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// update the commitIndex
		// 找到大多数已匹配的下标
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	//到这里，内置的心跳(日志复制)RPC函数才结束，下面才是正常执行的复制流程，只不过上面的回调函数的定义！
	//加锁执行
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//检测上下文是否丢失
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}
	//总逻辑就是为每一个从节点发送心跳、匹配日志，所以是一个大循环！
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}
		//查看主节点与从节点的这个下标是不是匹配！
		prevIdx := rf.nextIndex[peer] - 1
		//这里说明当前节点的日志还没有向app层apply，所以这里新开轻量级线程apply
		if prevIdx < rf.log.snapLastIdx {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIdx,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", peer, args.String())
			go rf.installToPeer(peer, term, args)
			continue
		}
		//如果是正常的下标的话，找到对应的任期
		prevTerm := rf.log.at(prevIdx).Term
		//构造主节点向从节点追加日志的参数
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,           //主节点的当前任期
			LeaderId:     rf.me,                    //主节点的当前id
			PrevLogIndex: prevIdx,                  //从节点开始匹配的下标
			PrevLogTerm:  prevTerm,                 //从节点开始匹配的任期
			Entries:      rf.log.tail(prevIdx + 1), //从节点的从prevIdx开始的后面的所有日志
			LeaderCommit: rf.commitIndex,           //主节点已提交至app层的下标
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		//到这里才开始真正的向从节点发送心跳/复制日志
		go replicateToPeer(peer, args)
	}
	return true
}

// PartA 心跳逻辑:
// PartA 心跳逻辑: 细节1: 成为Leader后等间隔的发起心跳逻辑
// could only replcate in the given term
func (rf *Raft) replicationTicker(term int) {
	// PartA 心跳逻辑: 细节2：心跳前检测上下文是否变化，准确来说就是看自己还是不是当前任期的L！
	for !rf.killed() {
		ok := rf.startReplication(term) //心跳的同时检测上下文
		if !ok {
			break
		}

		time.Sleep(replicateInterval)
	}
}
