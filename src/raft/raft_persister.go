package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
)

// print the persister Info state from the failover
func (rf *Raft) persisterString() string {
	return fmt.Sprintf("T%d, VoteFor: %d, Log: [0:%d)", rf.currentTerm, rf.votedFor, rf.log.size())
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistLocked() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	//构造序列化类e的实例
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.log)在PartD弃用了
	rf.log.persist(e) //启动日志的持久化函数
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot) //将snapshot进行持久化！
	//一个helper函数，打印持久化的东西到底是什么！
	LOG(rf.me, rf.currentTerm, DPersist, "Persist: %v", rf.persisterString())
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm, VotedFor int
	//分别分开写，这样报错就容易看出
	if err := d.Decode(&currTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read Persist err:%v", err)
		return
	}
	rf.currentTerm = currTerm

	if err := d.Decode(&VotedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor err:%v", err)
		return
	}
	rf.votedFor = VotedFor

	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log err:%v", err)
		return
	}
	rf.log.snapshot = rf.persister.ReadSnapshot()
	if rf.commitIndex < rf.log.snapLastIdx {
		rf.commitIndex = rf.log.snapLastIdx
		rf.lastApplied = rf.log.snapLastIdx
	}
	//print the failover info
	LOG(rf.me, rf.currentTerm, DPersist, "Read from disk: %v", rf.persisterString())
}
