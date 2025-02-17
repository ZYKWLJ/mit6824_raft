这里就是一个单纯的基于Raft的强一致的K-V数据库，也没有进行分片。
key：commitIndex(即Raft集群保存的数据提交下标)
value：实际的客户端传来的值