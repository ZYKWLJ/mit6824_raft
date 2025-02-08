package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		//用于线程同步的条件变量操作,原理是释放当前持有的锁（即 rf.mu），并将当前线程阻塞，
		//直到其他线程调用 rf.applyCond.Signal() 或 rf.applyCond.Broadcast() 方法来唤醒该线程。
		//这里就像转账一样，自己的账号A想给B转账800，但是现在自己的钱不够了只有600，所以自己会一直阻塞，直到其他账号(例如C)给我转了200以上，我才被唤醒，我再转给B。
		rf.applyCond.Wait()
		entries := make([]LogEntry, 0) //定义日志条目
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log[i]) //追加日志条目
		}
		rf.mu.Unlock()
		//注意对构造的apply message进行apply
		for i, entry := range entries {
			//这里是日志应用的信息！
			//遍历 entries 切片，将切片中的每个元素封装成 ApplyMsg 结构体的实例，然后通过 rf.applyCh 通道发送出去
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + i + 1, //here must be cautious
			}
		}
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries) //提交日志长度必定是加上所有的@！
		rf.mu.Unlock()
	}
}
