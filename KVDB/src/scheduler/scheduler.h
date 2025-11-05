/*
resource scheduler 
编排全局资源
提供以下能力：
1）主动绑定线程到numa 节点； 
2）资源使用状态，numa核心空闲度/利用率等； 
3）自定义/自适应的绑定策略。
scheduler 为单例模型，全局一个，使用get_instance获取。
*/