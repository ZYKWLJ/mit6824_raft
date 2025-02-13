package raft

// 为了保证所有的 apply 串行执行，我们将所有的 apply 逻辑都收束到 applicationTicker 线程中。
// 为此，我们在 Raft 结构体中新引入一个标记变量 snapPending，当 Follower 收到 snapshot 时，就设置该标记，
// 并且通过 rf.applyCond 唤醒 applicationTicker 进行 apply。
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		//用于线程同步的条件变量操作,原理是释放当前持有的锁（即 rf.mu），并将当前线程阻塞，
		//直到其他线程调用 rf.applyCond.Signal() 或 rf.applyCond.Broadcast() 方法来唤醒该线程。
		//这里就像转账一样，自己的账号A想给B转账800，但是现在自己的钱不够了只有600，所以自己会一直阻塞，直到其他账号(例如C)给我转了200以上，我才被唤醒，我再转给B。
		rf.applyCond.Wait()
		entries := make([]LogEntry, 0) //定义日志条目
		snapPendingInstall := rf.snapPending
		if !snapPendingInstall {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entries = append(entries, rf.log.at(i)) //追加日志条目
			}
		}
		rf.mu.Unlock()
		//这里为什么需要三段式写？因为apply是上网络了，是一个阻塞的操作，需要异步进行，不需要加锁，防止死锁
		//注意对构造的apply message进行apply
		if !snapPendingInstall {
			for i, entry := range entries {
				//这里是日志应用的信息！
				//遍历 entries 切片，将切片中的每个元素封装成 ApplyMsg 结构体的实例，然后通过 rf.applyCh 通道发送出去
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + i + 1, //here must be cautious
				}
			}
		} else { //如果Follower收到了来自Leader的快照复制，就将这个快照数据apply到应用层
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIdx,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}
		rf.mu.Lock()
		if !snapPendingInstall {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Install Snapshot for [0, %d]", rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false //一定要注意复原，不然会反复地apply
		}
		rf.mu.Unlock()
	}
}
