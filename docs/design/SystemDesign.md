# 设计原则
* Simple is beautiful!
* 云原生
* 多租户
* 存储与计算分离架构
* 原生流批一体
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
* client: 通过grpc实现的openmessage客户端


