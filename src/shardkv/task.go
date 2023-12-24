package shardkv

import "time"

// 处理 apply 任务
func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var opReply *OpReply
				raftCommand := message.Command.(RaftCommand)
				if raftCommand.CmdType == ClientOpeartion {
					// 取出用户的操作信息
					op := raftCommand.Data.(Op)
					if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
						opReply = kv.duplicateTable[op.ClientId].Reply
					} else {
						// 将操作应用状态机中
						shardId := key2shard(op.Key)
						opReply = kv.applyToStateMachine(op, shardId)
						if op.OpType != OpGet {
							kv.duplicateTable[op.ClientId] = LastOperationInfo{
								SeqId: op.SeqId,
								Reply: opReply,
							}
						}
					}
				} else {
					opReply = kv.handleConfigChangeMessage(raftCommand)
				}

				// 将结果发送回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				// 判断是否需要 snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 获取当前配置
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		kv.mu.Lock()
		newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
		kv.mu.Unlock()

		// 传入 raft 模块进行同步
		kv.ConfigCommand(RaftCommand{ConfigChange, newConfig}, &OpReply{})
		time.Sleep(FetchConfigInterval)
	}
}
