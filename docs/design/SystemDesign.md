# 设计原则
* Simple is beautiful!
* 云原生
* 多租户
* 存储与计算分离架构
* 原生流批一体
* 多机房容灾模式
* 前期存储使用对象存储, 原生的分层存储设计, 不实现过于复杂的副本机制   
* 支持丰富的插件化拓展
  * 插件化多协议(前期不再独创自己的消息队列协议, 并且逐步去支持现有的其他消息队列协议, 以openmessage为主, 逐步拓展至kafka/rocketmq/pulsar/MQTT/AMQP等协议的适配器
  * 可拓展的元数据设计: zookeeper/openmessage-dleger等其他存储插件
  * 存储也支持可拓展
  
# 技术选型
前期需要快速落地, 敏捷开发, 因此技术选型有下列几种:
* 网络通信使用grpc实现
* 客户端协议使用openmessaging
* 数据存储使用对象存储, 本地测试时通过minio进行

# 技术架构
核心组件:
* broker: less 状态的存储节点
  * 负责数据写入
  * 数据本地缓存加速
  * 元数据写入
* proxy: 无状态计算层
  * 客户端负责服务发现
  * 提供生产与消费能力
  * 提供broker服务发现能力
* zookeeper
  * 提供broker与proxy的节点发现
  * 提供元数据存储
* client: 通过grpc实现的openmessage客户端

# 逻辑架构
* ClusterSet 集群集, 代表多个idc/region的集群
* Cluster 代表单个region的集群实例
* Tenant 租户
* Namespace 命名空间
* Topic 逻辑上的topic, 归属于一个命名空间, 代表一个或多个partition合集
* Partition topic上的某个分区, 可以在末端进行写入, 并行进行消费
* Message 每个message代表一条独立的消息, 带有一个自增的唯一messageId

# 与数据湖结合的思考
原始流批一体消息队列, 消息队列写入即所得, 可以兼顾准确性与实时性
Iceberg 隐式分区可对应时间范围
Iceberg中没有bucket的概念, 消息队列的分区需要通过两个逻辑表来实现, 例如:
Topic t1, partition 0 -> iceberg table: t1__p-0

对外访问时, 可以通过消息队列进行统一封装

数据湖表过多时, 如何解决?

# 海量topic场景下的承载能力和瓶颈设想
可能受限于下面几个点:
* PowerMQ元数据
* 数据湖的元数据压力
* rocksdb单节点的性能瓶颈



