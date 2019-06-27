c++版仅需要链接 raft.lib 即可。

lraft 库导出 rawnode 及相关接口，并实现了 LDBStorage 用于在 lua 层实现 raft 的 storage，默认提供了一个 ldbstorage.lua 实现，依赖 lualeveldb（https://github.com/jxlczjp77/lua_leveldb.git） 库。

## lraft API

| 接口                               | 说明                           |
| ---------------------------------- | ------------------------------ |
| lraft.rawnode()                    | 创建 rawnode                   |
| lraft.config()                     | 创建 rawnode 配置              |
| lraft.memorystorage()              | 创建内存 storage               |
| lraft.ldbstorage()                 | 创建 ldbstorage                |
| lraft.IsLocalMsg()                 | 是否是本地消息（测试需要）     |
| lraft.IsEmptyHardState()           | 是否是空 HardState（测试需要） |
| lraft.readstate(Index, RequestCtx) | 创建 readstate                 |
| lraft.default_logger()             | 获取默认 logger                |

| rawnode 接口                     | 类型 | 说明                   |
| -------------------------------- | ---- | ---------------------- |
| rawnode:init(cfg, peers)         | 函数 | 初始化节点             |
| rawnode:ready()                  | 函数 | 获取节点 ready 数据    |
| rawnode:step(msg)                | 函数 | 处理消息               |
| rawnode:advance(rd)              | 函数 | 处理 ready 状态        |
| rawnode:campaign()               | 函数 | 节点进入选举状态       |
| rawnode:propose(data)            | 函数 | 发起一个提议           |
| rawnode:propose_confchange(conf) | 函数 | 发起一个配置改变提议   |
| rawnode:apply_confchange(conf)   | 函数 | apply 配置改变         |
| rawnode:readindex()              | 函数 | 获取 readindex         |
| rawnode:has_ready()              | 函数 | 是否有 ready 状态      |
| rawnode.id                       | 属性 | 获取节点 id            |
| rawnode.uncommitted_size         | 属性 | 获取未提交的数据大小   |
| rawnode.read_states              | 属性 | 获取 read_states       |
| rawnode.step_func                | 属性 | 测试用，设置 step 回调 |

| config 选项               | 说明                            |
| ------------------------- | ------------------------------- |
| ID                        | 节点 ID                         |
| peers                     | 其他节点 ID 列表：{1, 2, 3}     |
| learners                  | 其他学习节点 ID 列表：{4, 5, 6} |
| ElectionTick              | 选举间隔                        |
| HeartbeatTick             | 心跳间隔                        |
| Applied                   |                                 |
| MaxSizePerMsg             |                                 |
| MaxCommittedSizePerReady  |                                 |
| MaxUncommittedEntriesSize |                                 |
| MaxInflightMsgs           |                                 |
| CheckQuorum               |                                 |
| PreVote                   |                                 |
| Storage                   |                                 |
| ReadOnlyOption            |                                 |
| DisableProposalForwarding |                                 |
