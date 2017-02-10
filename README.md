
# Abstract
Zsls(Zeus-Light-Schedule) 是一个分布式作业调度系统，支持实时任务和定时任务。有较好的伸缩性，扩展性以及健壮稳定性。</br>
Zsls支持的任务类型：</br>
1. 实时任务：提交之后立即分发执行，类似消息队列，消息持久化，可堆积。</br>
2. 定时任务：支持间隔一定时间重复执行和cron表达式。与Quartz类似。</br>
* * *
Zsls(Zeus-Light-Schedule) is a distributed job-schedule system supporting real-time jobs and cron-style jobs, with good scalability and robustness.</br>
Job types that zsls supports:</br>
1. RealTime-Job: dispatched immediately after received by server, like a message queue. Jobs can be persisted and accumulated in server.</br>
2. Cron-Job: quartz-style job, implemented with quartz.</br>

</br>

# Design
### architecture diagram
Zsls采用Master-Worker中心式架构。JobTracker是调度中心，TaskTracker负责任务执行。</br>
* * *
Zsls uses Master-Worker architecture, JobTracker for job schedule and TaskTracker for task execution.</br>
![](https://github.com/squallyou/zsls/blob/master/pics/arc.png)</br>
### components
* SchedClient：客户端，负责向JobTracker提交作业，接受反馈。</br>
* JobTracker：调度中心，负责作业存储、调度、任务分发与结果收集，并提供对外查询，人工干预等服务。</br>
* TaskTracker：任务节点，负责具体任务执行。</br>
* Admin：后台管理，负责节点管理，集群管理，作业监控，人工干预等。</br>

- - - -
* SchedClient: client used to commit jobs to JobTracker.</br>
* JobTracker: schedule server, for jobstore, task schedule and task dispatch. also provide query and other services to users.</br>
* TaskTracker: worker that execute tasks.</br>
* Admin: a simple web system to monitor and manapulate nodes and jobs.</br>

</br>

# Features
1.	**接口扩展**</br>
系统将注册中心与作业存储的概念进行了抽象，方便其进行扩展。目前注册中心支持Zookeeper(未来可扩展redis)，作业存储目前支持Mysql和Oracle。</br>
2.	**高可用**</br>
JobTracker支持HA和状态恢复，通过注册中心和任务存储实现。</br>
3.	**故障转移**</br>
TaskTracker宕机时，JobTracker会回收其正在执行的任务并分配给其他节点。</br>
4.	**工作流与条件表达式**</br>
Zsls依据“阉割版”的BPMN规范实现了一个流引擎，并允许用户通过配置条件表达式进行任务流的控制。</br>
5.	**作业运行方式**</br>
任务类型支持Java和Shell两种。前者适用于类似消息队列的高频短时间任务，在线程池内执行；后者更适用于耗时长的作业或批处理作业，开启子进程执行。因此系统可以支持c++等多语言产生的作业，只要最终部署好并配置启动脚本即可。</br>
6.	**业务隔离**</br>
系统引入“域”的概念进行业务隔离。配置时可以指定任务在哪个域或者哪个节点上执行。假如配置了域，会通过负载均衡将任务分配给域内某个节点执行；否则分配指指定节点执行。</br>
7.	**任务分片**</br>
作业内的任务可以配置分片。具有分片配置的任务会按照配置进行分片产生多个分片任务，然后将不同的分片分配给不同的节点执行。分配时会附带上分片编号。</br>
8.	**系统监控和管理**</br>
Admin后台管理，可以监控当前所有的工作节点及运行的任务。可杀掉作业（仅shell类型），强制任务完成，重做任务等</br>

* * * *

1. **Module Extension**</br>
Interfaces desinged for RegistryCenter and JobStore. For now Zookeeper is the only implementation for Registry(redis maybe supported in the future), while Mysql and Oracle are surpported for JobStore.</br>
2. **HA**</br>
HA and recover are supported.</br>
3. **Fail Over**</br>
JobTracker would take back and re-assign the unfinished tasks when a TaskTracker fails.</br>
4. **JobFlow**</br>
Zsls has a job engine that support a simple BPMN-style specification. Also condition expressions are provided to enable users to control the flow.</br>
5. **How tasks runs**</br>
For now two kinds of tasks are supported in TaskTracker: Java task and Shell task. Java tasks are executed within TaskTracker's thread pool, suitable for high-frequency and short-period tasks, while Shell tasks are executed in forked sub-processes. The later one enables our system to be applied to avariaty kinds of jobs, despite the languages in which the task was written, as long as the start script is properly configured.</br>
6. **Transaction Isolation**</br>
The concept of "domain" is introduced in Zsls to organize homogeneous worker nodes. Either domain or target node should be specified for each task when committing a job, and in former case, the task would be assigned to a random worker with the specified domain.</br>
7. **Task Partition**</br>
A task would be splitted into multiple tasks if its partition property is configured. The partition index is included in task parameters when assigned to TaskTracker.</br>
8. **Montor and Management**</br>
monitor all worker nodes and jobs, commit/kill a job(only Shell type), mark task finished, re-do a task, etc.
