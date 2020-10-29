# 6.824 Distributed Systems

## LEC 1

### MapReduce

目标是让普通的程序员简单地编写分布式程序，只需要定义 Map 和 Reduce 函数。MR 负责和隐藏了分布式的所有方面，使其几乎与编写一般的顺序执行程序没有区别。

MR 隐藏了很多细节: 

1. 向服务器发送 app code
2. 跟踪任务的完成情况
3. 将数据从 Map 移动到 Reduce
4. 服务期间的负载均衡
5. 出错处理

同时 MR 也有一些限制: 

1. 没有交互或者是状态这一类的东西，简单来说就是各个任务之间是独立的
2. 不能有迭代，不能有多状态的管线
3. 不能做实时和流处理

输入和输出储存在 GFS( Google File System) 中。

一些细节( Master，一个分发任务，同时记录这些进程 ): 

1. master 一直分发任务，直到所有任务完成。Maps 将输出写入到本地磁盘，Maps 使用哈希分割输出到每一个 Reduce 任务。
2. 所有 Maps 完成之后，master 分配 Reduce 任务。每个 Reduce 从所有 Map worker 获得输出，每个 Reduce 任务向 GFS 中的独立的文件写入输出。

MR 减小网络负载的方法: 所有电脑都同时运行 GFS 和 MR worker，所以输入从本地磁盘读入，不需要网络。中间数据只通过网络传输一次。Map worker 将输出写入到本地磁盘，Reduce worker 直接从 Map worker 读入数据，而不是 GFS（刚刚不还说都通过 GFS 吗，怎么又说走本地了？）。

负载均衡：很多机器等一个比较慢的机器是非常效率低的，但是有些任务的耗时就是要长一些。解决方法：使任务的数量比 worker 多很多，master 可以一直给完成任务的 worker 分配新任务。同时使得没有任务大到足够堵塞整个流程。最终效果就是快的 server 做更多的任务，所有 server 完成所有任务的时间相近。

MR 的错误容忍: 

MR 相对程序员完全隐藏错误。MR 只会重跑失败的 Map 和 Reduce。因为正确性要求重新执行要有完全相同的结果，所以 Map 和 Reduce 函数必须使完全确定的，他们只能通过自己的参数与外界沟通，没有 state，没有文件 IO，没有交互，没有与外界的交流。如果想要允许 non-functional 的 Map 或者 Reduce，那么出错了之后就只能整个工作重跑一遍，或者需要设置 synchronized global checkpoints。

现在 Google 用 Flume / FlumeJava 代替了 MapReduce；Colossus 和 Bigtable 代替了 GFS

