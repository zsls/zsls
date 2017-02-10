Zsls(Zeus-Light-Schedule) 是一个分布式作业调度系统，支持实时任务和定时任务。有较好的伸缩性，扩展性以及健壮稳定性。</br>
Zsls(Zeus-Light-Schedule) is a distributed job-schedule system supporting real-time jobs and cron-style jobs, with good scalability and robustness.</br>

</br>

Zsls支持的任务类型：</br>
1.	实时任务：提交之后立即分发执行，类似消息队列，消息持久化，可堆积。</br>
2.	定时任务：支持间隔一定时间重复执行和cron表达式。与Quartz类似。
</br>
Job types that zsls supports:</br>
1.  RealTime-Job: dispatched immediately after received by server, like a message queue. Jobs can be persisted and accumulated in server.</br>
2.  Cron-Job: quartz-style job, implemented with quartz.</br>

</br>

# Design
Zsls采用Master-Worker中心式架构。JobTracker是调度中心，TaskTracker负责任务执行。</br>
Zsls uses Master-Worker architecture, JobTracker for job schedule and TaskTracker for task execution.</br>
![](https://github.com/squallyou/zsls/blob/master/pics/arc.png)
