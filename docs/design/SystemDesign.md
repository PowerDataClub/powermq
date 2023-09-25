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
  