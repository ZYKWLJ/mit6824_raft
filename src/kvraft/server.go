package kvraft

import (
	"bytes"
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// 客户端需要一直等待sever的大多数节点都 committed 这条消息了，leader 就可以apply这条日志到状态机，并将结果返回给客户端
type KVServer struct {
	mu sync.Mutex
	me int
	rf *raft.Raft
	//从 applyCh 中接收来自 raft 的日志消息，处理完之后，将结果存储到一个 channel 中，这时候 Get/Put/Append 方法就能从这个 channel 中获取到结果，并返回给客户端。
	applyCh chan raft.ApplyMsg //这里定义一个存放日志信息(快照)的通道
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this size(达到了这个尺寸就快照)阈值设置

	// Your definitions here.
	lastApplied    int
	stateMachine   *MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply       //这里是为每一个Index创建一个channal，里面存储着信息。使用映射创建！
	duplicateTable map[int64]LastOperationInfo //去重表，用于防止重复请求。key是最后一个请求的ID。
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, OpType: OpGet})

	// 如果不是 Leader 的话，直接返回错误，客户端会进行重试
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	//这里是结果保存在这个通道里面
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	//将通道里面的数据发送到result保存后再赋值给reply结构体
	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// 分布式系统的线性一致性处理=>去重（防止数据覆盖！）
// 线性一致性要求每个客户端的请求应该立即被执行，并且只被执行一次。因为如果在分布式 KV 存储系统中，当一个 Put 请求在执行过程中因为节点故障而失败时，
// 客户端通常会进行重试。然而，如果在重试之前系统状态已经发生了改变（就像例子中 Put(x, 2) 已经成功执行），那么重试的 Put(x, 1) 操作会覆盖掉最新的状态，
// 对于重试的请求，我们应该如何避免其被执行多次呢？
// 用于判断是不是重复请求了？
func (kv *KVServer) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId //表示请求到了并且请求的序列号小于之前存过得序列号，说明这是重复请求，直接返回即可，防止再次IO了
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 判断请求是否重复
	kv.mu.Lock()
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求，直接从去重表中返回结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOperationType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 删除通知的 channel
	// 因为index是唯一的，所以可以直接删除对应的chan，并且使用异步删除，防止阻塞！
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo) //去重表信息的初始化

	// 从 snapshot 中恢复状态
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	return kv
}

// 这里是单独的Raft的异步处理线程
// 处理 apply 任务
func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh: //专用与通道的Switch。这里是将KV存储通道里面的数据发送到message中来
			if message.CommandValid { //如果这个日志信息是有效的话
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				//更新最后的处理消息
				kv.lastApplied = message.CommandIndex

				// 取出用户的操作信息
				op := message.Command.(Op)
				var opReply *OpReply
				//根据操作类型和请求是否重复，来决定是直接返回之前处理过的响应，还是将操作应用到状态机中进行处理，并更新重复请求记录表。
				//幂等操作~
				//OpGet 操作一般是幂等的，也就是说无论执行多少次相同的 GET 请求，其结果都是一样的。所以对于幂等操作没必要判断是否重复操作，结果都是一样的！
				//OpPut、OpDelete等操作是非幂等的。多次执行相同的非幂等操作可能会导致系统状态发生多次改变，产生不符合预期的结果,所以，对于非幂等操作，需要判断请求是否重复，避免重复处理。
				if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
					fmt.Println("这是重复请求，直接从map表里面取出数据")
					opReply = kv.duplicateTable[op.ClientId].Reply //这里每一次请求的参数也不一定相等呀？咋搞的？
				} else {
					// 将操作应用状态机中
					opReply = kv.applyToStateMachine(op)
					if op.OpType != OpGet {
						//更新之前的请求表
						kv.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 前面讲结果应用到了状态机里面，再将结果发送回去？发送回哪里去？
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply //将应用到状态机的结果发送到这个通道里面
				}

				// 判断是否需要 snapshot
				//kv.maxraftstate != -1 是为了确保启用了基于状态大小的快照机制。
				//判断当前 Raft 状态数据的大小是否已经达到或超过了预设的阈值 kv.maxraftstate
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
				//	kv实例从快照中恢复数据
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 持久化到状态机
func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	}
	return &OpReply{Value: value, Err: err}
}

// 拿到对应的Index的map里面的chan
func (kv *KVServer) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok { //说明 index 对应的通道还不存在。创建一个
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

// 删除这个chan
func (kv *KVServer) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.stateMachine)
	_ = enc.Encode(kv.duplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

// 从快照数据中恢复 KVServer 的状态。
func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var stateMachine MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	if dec.Decode(&stateMachine) != nil || dec.Decode(&dupTable) != nil {
		panic("failed to restore state from snapshpt")
	}

	kv.stateMachine = &stateMachine
	kv.duplicateTable = dupTable
}
