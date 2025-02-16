package kvraft

import "course/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	//存储的servers 列表，表示的是后端分布式 KV 服务的所有节点信息，我们可以通过这个信息去向指定的节点发送数据读写的请求。
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int // 记录 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader
	// clientID+seqId 确定一个唯一的命令=>详解？
	clientId int64
	seqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}

	for {
		var reply GetReply
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败，选择另一个节点重试
			//(注意一开始只能是轮询~后面才保留的LeaderId，才可以直接查找)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 调用成功，返回 value
		return reply.Value
	}
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败，选择另一个节点重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 调用成功，返回
		ck.seqId++ //这里的序列号是指？
		return
	}
}

// 这里合二(put-append)操作为一，并且根据op参数来选择不同操作而已！
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
