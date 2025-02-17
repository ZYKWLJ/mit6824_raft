package shardctrler

//
// Shardctrler clerk.
//

import "course/labrpc"
import "crypto/rand"
import "math/big"

// 配置集群的客户端，即为访问配置集群服务的外露接口
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId int
	clientId int64
	seqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 客户端的初始化
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk) //new 一个客户端
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		//返回最新的配置信息
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.Servers = servers

	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++ //这里对数据有更改，需要进行持久化，所以保存了唯一的数据序列号
		return
	}
}

// 参数是一个集群中的 Group ID，表示这些 Group 退出了分布式集群
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}
