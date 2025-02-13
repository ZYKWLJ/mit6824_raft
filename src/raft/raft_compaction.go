package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

// Leader给follower发送快照同步的RPC的请求的参数
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// Follower接受Leader发送的日志快照复制请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())
}

// Leader发送给Follower的日志快照复制请求
func (rf *Raft) SendInstallSnapshot(sever int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[sever].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallToPeer(peer, term int, args *InstallSnapshotArgs) {
	//内置嵌套函数！
	//这里构造发送快照同步请求的参数
	//installToPeer := func(peer int, args *AppendEntriesArgs) {
	reply := &InstallSnapshotReply{}
	//先检测是否能成功发送RPC
	ok := rf.SendInstallSnapshot(peer, args, reply)
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
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
	//	note:we needn't updata the commitIndex
	//	because the snapshot must include the commitIndex
	
}
