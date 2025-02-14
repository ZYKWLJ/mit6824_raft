package raft

import (
	"course/labgob"
	"fmt"
)

// PartD:快照处理，原因是单个单个的日志RPC太麻烦了，直接将之前的日志序列化为快照进行一次RPC，减少多次网络请求带来的开销！
type RaftLog struct {
	//这两个字段是快照头结点的信息
	snapLastIdx  int
	snapLastTerm int
	//contains [1,snapshot]	(because there is a Dummy node)
	//这里是快照(压缩的部分)
	snapshot []byte
	//contains index (snapLastIdx,snapLastIdx+len(tailLog)-1] for real data
	//contains index snapLastIdx for mock log entry
	//快照之后的真实数据！
	tailLog []LogEntry
}

// 入口1:构造函数构造日志
func NewLog(snapLastIdx int, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

// all the functions below should be called under the protection of rf.mutex
// 另一个入口：宕机重启的反序列化构造日志
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last index failed")
	}
	rl.snapLastIdx = lastIdx
	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last term failed")
	}
	rl.snapLastTerm = lastTerm
	var entries []LogEntry
	if err := d.Decode(&entries); err != nil {
		return fmt.Errorf("decode entries failed")
	}
	rl.tailLog = entries
	return nil
}

// log的专属序列化函数
func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// index convertion
// 下标转换函数(现在只有在快照点之后才能访问了！
// 因为应用层在 index 处做了个快照，Raft 层帮我把该快照保存下，同时，index 以及之前的日志就可以释放掉了。)
func (rl *RaftLog) size() int {
	return len(rl.tailLog) + rl.snapLastIdx
}

// 实际能够访问到的下标(此部分在快照后面的)
// 逻辑下标logicIdx是整个长度的
// idx是针对快照的起始点的下标
func (rl *RaftLog) idx(logicIdx int) int {
	//	if the logicIdx fall beyond [snapLastIdx,size()-1]
	if logicIdx < rl.snapLastIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("logic index out of range=>%d id out of [%d, %d]", logicIdx, rl.snapLastIdx, rl.size()-1))
	}
	return logicIdx - rl.snapLastIdx
}

// 按下标访问非快照后面的日志的条目
func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

// 快照之后的第一条日志！
func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIdx
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.snapLastIdx+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.snapLastIdx+len(rl.tailLog)-1, prevTerm)
	return terms
}

// snapshot in the index
// do checkpoint from the app layer
// 这是应用层从上往下传给Raft(Leader)层的
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)
	rl.snapLastIdx = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot
	//	重新创建一个新的日志数组
	//make函数在go里面是内置的用于创建切片、映射、通道三种引用类型的函数
	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx) //用于指定创建对象的初始大小和容量
	newLog = append(newLog, LogEntry{                       //先加入空日志为起始
		Term: rl.snapLastTerm,
	})
	//现在开始重新复制。
	//... 是展开操作符。当它用于切片时，会把切片中的元素展开。在 append 函数里使用展开操作符，就可以将一个切片的所有元素追加到另一个切片中。
	//这行代码调用 append 函数，将 rl.tailLog[idx:] 切片展开后的所有元素追加到 newLog 切片的末尾，然后把追加后的结果赋值给 newLog
	newLog = append(newLog, rl.tailLog[idx:]...)
	rl.tailLog = newLog
}

// do checkpoint from the app layer
// 这是follower层从下往上传给应用层的。这里的思路是直接覆盖，所以这直接覆盖就好了，无需复制什么的！
func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot
	//	重新创建一个新的日志数组
	//make函数在go里面是内置的用于创建切片、映射、通道三种引用类型的函数
	newLog := make([]LogEntry, 0, 1)  //用于指定创建对象的初始大小和容量
	newLog = append(newLog, LogEntry{ //先加入空日志为起始
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}

// mutate method
// 将日志追加在tailLog后面
func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

// 在特定下标处追加
func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIndex+1)], entries...)
}

// 返会后面的所有日志
func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx > rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIdx):]
}

// 高频辅助函数，获取tailLog最后一条的日志的下标和任期
func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIdx + i, rl.tailLog[i].Term
}
