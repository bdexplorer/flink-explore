状态化流处理概述

## 状态化流处理

几乎所有数据都是以连续事件流的形式产生。

任何一个处理事件流的应用，如果要支持跨多条记录的转换操作，都必须是有状态的，能够存储和访问中间结果。

Apache Flink会将应用状态存储在本地内存或嵌入式数据库中。Flink需要对本地状态予以保护，为此，Flink会定期将应用状态的一致性检查点写入远程持久化存储。

![image-20211009073340230](picture/状态保存.png)

有状态的流处理应用通常会从事件日志中读取时间记录。事件日志负责存储事件流并将其分布式化。一旦出现故障，Flink会利用之前的检查点恢复状态并重置事件日志的读取位置，以此来是有状态的流处理应用恢复正常。

![image-20211010090930191](picture/flink应用.png)



## 事件驱动型应用

事件驱动型应用是一类通过接收事件流触发特定应用业务逻辑的有状态的流式应用。

- 实时推荐
- 模式识别或复杂事件处理
- 异常检测

事件驱动型应用利用日志进行通信，其数据则会以本地状态的形式存储。

事件驱动型应用有很多优势：

1. 访问本地状态的性能要比读写远程数据存储系统更好
2. 伸缩性和容错交由流处理引擎完成
3. 以事件日志作为应用的输入，不但完整可靠，而且还支持精准的数据重放。
4. Flink可以将应用状态重置到之前的某个检查点，从而允许应用在不丢失状态的前提下更新或扩缩容。

## 数据管道

有状态的流处理应用的另一个日常应用是以低延迟的方式获取、转换并插入数据，此类应用称为数据管道。

## 流式分析

流式分析应用不需要等待周期性地触发，相反，它会持续获取事件流，以极低的延迟整合最新事件，从而可以不断更新结果。

## Flink快览

1. 同时支持事件时间和处理时间语义。事件时间语义能够针对无序事件提供一致、精确的结果；处理时间语义能够用在具有极低延迟的需求的应用。
2. 提供精确一致的状态一致性保障
3. 在每秒处理百万条事件的同时保持毫秒级延迟
4. 层次化API
5. 常用的存储系统连接器
6. 高可用配置
7. 允许在不丢失应用状态的前提下更新作业的程序代码。

# 流处理基础

### Dataflow图

![image-20211010091800165](picture/dataflow.png)

Dafaflow程序描述了数据如何在不同操作之间流动。Dataflow程序通常表示为有向图。图中的顶点称为算子，表示计算；而边表示数据依赖关系。

没有输入端的算子称为数据源，没有输出端的算子称为数据汇。一个Dataflow至少要有一个数据源和一个数据汇。

Dataflow是一个逻辑图，为了执行程序需要将逻辑图转换为物理Dataflow图，后者会指定程序的执行细节。例如：每个算子的并行任务数。在物理Dataflow中顶点表示任务。

![image-20211010092041598](picture/物理dataflow.png)

### 数据并行和任务并行

数据并行：将输入数据分组，让同一操作的多个任务并行执行在不同的数据子集上

任务并行：让不同算子的任务并行计算。

### 数据交换策略

![image-20211010094155143](picture/数据交换策略.png)

转发策略：在发送端任务和接收端任务之间一对一地进行数据传输。如果两端任务运行在同一物理机上，该交换策略可以避免网络通信。

广播策略：会把每个数据项发往下游算子的全部并行任务。涉及网络通信，并且十分昂贵。

基于键值的策略：根据某一键值属性对数据分区，并保证键值相同的数据项会交由同一任务处理。

随机策略：会将数据均匀分配至算子的所有任务，以实现计算任务的负载均衡。

## 并行流处理

### 延迟和吞吐

延迟：处理一个事件所需的时间。低延迟是流处理的一个关键特性，它滋生了所谓的实时应用。

吞吐：用来衡量系统处理能力的指标，它告诉我们系统每单位时间可以处理多少事件。

关注点：峰值吞吐量

背压：如果系统持续以力不能及的高速率接收数据，那么缓冲去可能会被用尽，继而导致数据丢失。

延迟和吞吐会相互影响。

### 数据流上的操作

流处理引擎通常会提供一系列内置操作来实现数据流的获取、转换、以及输出。这些操作可以是无状态的，也可以是有状态的。

无状态的操作不会维持内部状态，即处理事件时无需依赖已处理过的事件，也不保留历史数据。

有状态的算子可能需要维护之前接收的事件信息。它们的状态会根据传入的事件更新，并用于未来事件的处理逻辑中。有状态的流处理应用在并行化和容错方面会更具挑战性，因为它们需要对状态进行高效划分，并且在出错时需要进行可靠的故障恢复。

![image-20211010095808014](picture/数据流操作.png)

#### 数据接入和数据输出

#### 转换操作

转换操作是一类“只过一次”的操作，它们会分别处理每个事件。这些操作逐个读取事件，对其应用某些转换并产生一条新的输出流。

#### 滚动聚合

滚动聚合（求和、最小值、最大值）会根据每个到来的事件持续更新结果。聚合操作都是有状态的，它们通过将新到来的事件合并到已有的状态来生成更新后的聚合值。

#### 窗口操作

有些操作必须收集并缓冲记录才能计算结果。例如流式join或像是求中位数的整体聚合。

窗口操作还支持在数据流上完成一些具有切实语义价值的查询。比如5min内的交通状况。

窗口操作会持续创建一些称为“桶”的有限事件集合，并允许我们基于这些有限集进行计算。窗口的行为是由一系列策略定义的，这些窗口策略决定了什么时间创建桶，事件如何分配到桶中以及桶内的数据什么时间参与计算。当触发条件满足时，桶内数据会发送给一个计算函数，由它来对桶中的元素应用计算逻辑。

- 滚动窗口——将事件分配到长度固定且互不重叠的桶中。在窗口边界通过后，所有事件会发送给计算函数进行处理。
- 滑动窗口——将事件分配到大小固定且允许相互重叠的桶中。
- 会话窗口——在线分析用户行为。会话窗口根据会话间隔将事件分为不同的会话，该间隔定义了会话在关闭前的非活动时间长度。

窗口操作与流处理中两个核心概念密切相关：时间语义和状态管理

## 时间语义

### 处理时间

处理时间是当前流处理算子所在机器上的本地时钟时间。基于处理时间的窗口会包含那些恰好在一段时间内到达窗口算子的事件，这里的时间是按照机器时间测量。

### 事件时间

事件时间是数据流中事件实际发生的时间，它以附加在数据流中事件的时间戳为依据。这些时间戳通常在事件数据进入流处理管道之前就存在，即便事件有延迟，事件时间窗口也能准确地将事件分配到窗口中，从而反映出真实发生的情况。

事件时间将处理速度和结果内容彻底解耦。基于事件时间的操作是可预测的，其结果具有确定性。

使用事件时间的挑战之一就是处理延迟事件。

依靠事件时间，可以保证在数据乱序的情况下结果依然正确，而且结合可重放的数据流，时间戳所带来的的确定性允许你对历史数据进行“快进”。

### 水位线

解决：怎样决定事件时间窗口的触发机制

水位线是一个全局进度指标，表示我们确信不会再有延迟事件到来的某个时间点。当一个算子收到时间为T的水位线，就可以认为不会再收到任何时间戳小于或等于T的事件。相当于收到某个信号：某个特定时间区间的时间戳已经到齐，可以触发窗口计算或对接收的数据进行排序了。

水位线允许我们再结果的准确性和延迟之间做出取舍。激进的水位线策略保证了低延迟，但随之而来的是低可信度。如果水位线过于保守，虽然可信度得以保证，但可能会无谓地增加处理延迟。

需要提供某些机制来处理那些可能晚于水位线的迟到事件。

### 处理时间和事件时间

处理时间窗口能将延迟降到最低。

事件时间能够保证结果的准确性，并允许处理延迟甚至无序的事件。

## 状态和一致性模型

状态管理：系统需要高效地管理状态并保证它们不受并发更新的影响。

状态划分：由于结果需要同时依赖状态和到来的事件，所以状态并行化会变得异常复杂。

状态恢复：有状态算子需要保证状态可以恢复，并且即使出现故障也要确保结果的正确。

# Apache Flink 架构

## 系统架构

Flink搭建需要4个不同的组件：JobManager、ResourceManager、TaskManager和Dispatcher

- JobManager：控制着单个应用程序的执行。换句话说，每个应用都由一个不同的JobManager掌控。JobManager可以接收需要执行的应用，该应用会包含一个所谓的JobGraph（逻辑Dataflow）以及打包了全部所需类、库以及其他资源的JAR文件。JobManager负责将JobGraph转化为ExecutionGraph的物理（物理Dataflow），该图包含了那些可以并行执行的任务。JobManager从ResourceManager申请执行任务的必要资源（TaskManager处理槽），一旦它收到足够数量的处理槽，它就会将ExecutionGraph中的任务分发给TaskManager来执行。在执行的过程中，JobManager还要负责所有需要集中协调的操作，比如创建检查点。

- 针对不同的环境和资源提供者（YARN、Mesos、Kubernetes）Flink提供了不同的ResourceManager。ResourceManager负责管理Flink的处理资源单元——TaskManager处理槽。当JobManager申请TaskManager处理槽时，ResourceManager会指示一个拥有空闲处理槽的TaskManager将其处理槽提供给JobManager。如果ResourceManager的处理槽无法满足JobManager的请求，则ResourceManager可以和资源提供者通信，让它们提供额外容器来启动TaskManager进程。ResourceManager还负责终止空闲的TaskManager
- TaskManager：Flink的工作进程。
- Dispatcher会跨多个作业运行，它提供了一个REST接口来让我们提交需要执行的应用。一旦某个应用提交执行，Dispatcher会启动一个JobManager并将应用转交给它。REST接口意味着Dispatcher这一集群的HTTP入口可以受到防火墙保护。Dispatcher还会启动一个WEB UI。某些应用的提交方式可能用不到Dispatcher。

![image-20211010102616732](picture/提交作业流程.png)

## 应用部署

框架模式：yarn

库模式：k8s

## 执行任务

![image-20211010105117191](picture/算子、任务及处理槽.png)

一个TaskManager允许同时执行多个任务。这些任务可以属于同一个算子（数据并行），也可以是不同算子（任务并行），甚至还可以来自不同的作业（作业并行）。

应用的并行度由算子的最大并行度决定，

TaskManager中的多个任务可以在同一个进程内高效地执行数据交换而无须访问网络。然而任务过于集中也会使TaskManager负载变高，继而导致性能下降。

TaskManager会在同一个JVM进程内以多线程的方式执行任务。和独立的进程相比，线程更加轻量并且通信开销更低，但无法严格地将任务彼此隔离。因此只要有一个任务异常，就有可能“杀死”整个TaskManager进程。如果将TaskManager配置为只有一个处理槽，则可以限制应用在TaskManager级别进行隔离。

## 高可用性设置

### TaskManager故障

如果TaskManager故障，这时候JobManager会向ResourceManager申请更多的处理槽。若无法完成，JobManager将无法重启应用，直至有足够可用的处理槽。应用的重启策略决定了JobManager以何种频率重启应用及重启尝试之间的等待时间。

### JobManager故障

Flink支持在原JobManager消失的情况下将作业的管理职责及元数据迁移到另一个JobManager。

JobManager在高可用模式下工作时，会将JobGraph以及全部所需的元数据（JAR）写入一个远程持久化存储系统中。此外JobManager还会把存储位置的路径地址写入zookeeper的数据存储。在应用执行的过程中，JobManager会接收每个任务检查点的状态句柄（存储路径）。在检查点即将完成时，如果所有任务已经将各自状态成功写入远程存储，JobManager就会将状态句柄写入远程存储，并将远程位置的路径地址写入zookeeper。

因此所有用于JobManager故障恢复的数据都存储在远程存储上面，而zookeeper持有这些存储位置的路径。

![image-20211010112718183](picture/作业元数据存储.png)

新接手的JobManager会执行以下步骤：

1. 向zookeeper请求存储路径，以获取JobGraph、JAR文件以及应用最新检查点在远程存储的状态句柄。
2. 向ResourceManager申请处理槽来继续执行应用。
3. 重启应用并利用最近一次检查点重置任务状态。

## Flink中的数据传输

TaskManager负责将数据从发送任务传输至接收任务。它的网络模块在记录传输之前会先将它们收集到缓冲区中。换言之，记录并非逐个发送的，而是在缓冲区中以批次的形式发送。

TaskManager都有一个用于收发数据的网络缓冲池（每个缓冲默认32KB大小），如果发送端和接收端的任务运行在不同的TaskManager进程中，它们就需要用到操作系统的网络栈进行通信。流式应用需要以流水线的方法交换数据，因此每对TaskManager之间都需要维护一个或多个永久的TCP连接来执行数据交换。在shuffle模式下，每个发送端任务都需要向任意一个接收任务传输数据。对于每一个接收端，TaskManager都要提供一个专用的网络缓冲区，用于接收其他任务发来的数据。

如果接收端的并行度是4，每个发送端任务至少需要4个网络缓冲区来向任一接收端任务发送数据。同理每个接收端任务也需要至少4个缓冲区来接收数据。

在shuffle或广播连接的情况下，每个发送任务都需要为每个接收任务提供一个缓冲区，因此缓冲区数量可达到相关算子任务数的平方级别。

当发送任务和接收任务在同一个TaskManager进程时，发送任务会将要发送的记录序列化到一个字节缓冲区中，一旦该缓冲区占满就会被放到一个队列里。接收任务会从这个队列里获取缓冲区并将其中的记录反序列化。不涉及网络传输。

### 基于信用值的流量控制

使用缓冲的缺点是会增加延迟。

基于信用值的流量控制机制：接收任务会给发送任务授予一定的信用值，其实就是保留一些用来接收它数据的网络缓冲。一旦发送端收到信用通知，就会在信用值所限定的范围内尽可能多地传输数据，并会附带积压量（已经填满准备传输的缓冲数目）大小。接收端使用保留的缓冲来处理收到的数据，同时依据各发送端的积压量信息来计算所有相连的发送端下一轮的信用优先级。

基于信用值的流量控制是Flink低延迟的重要一环。

### 任务链接

Flink采用一种名为任务链接的优化技术来降低某些情况下的本地通信开销。任务链的前提是多个算子必须有相同的并行度且通过本地转发通道相连。

可以通过配置关闭任务链功能。

## 事件时间处理

### 时间戳

在事件时间模式下，Flink流式应用处理的所有记录都必须包含时间戳。时间戳将记录和特定的时间点关联，这些时间点通常是记录所对应事件的发生时间。实际可以自由选择，只要保证流记录的时间戳会随着数据流的前进大致递增。

Flink内部使用8位Long对时间戳编码。

### 水位线

Flink基于事件时间的应用还必须提供水位线（watermark）水位线用于在事件时间应用中推断每个任务当前的事件时间。基于时间的算子会使用这个时间来触发计算并推动进度前进。

水位线的基本属性：

1. 必须单调递增
2. 和记录的时间戳存在联系。一个时间戳为T的水位线表示，接下来所有记录的时间戳一定都大于T。

第二个属性可以用来处理数据流中时间戳乱序的记录。

当一个任务收到一个违反水位线属性，即时间戳小于或等于前一个水位线的记录时，该记录本应参与的计算可能已经完成。称此类记录为迟到记录。Flink提供了处理迟到记录的机制。

### 水位线传播和事件时间

Flink内部将水位线实现为特殊的记录，它们可以通过算子任务进行接收和发送。任务内部的时间服务会维持一些计时器，它们依靠接收到水位线来激活。这些计时器是由任务在事件服务内注册，并在将来的某个时间点执行计算。

当任务接收到一个水位线时，会执行以下操作：

1. 基于水位线记录的时间戳更新内部事件时间时钟。
2. 任务的时间服务会找出所有触发时间小于更新后的内部事件时间的计时器。对于每个到期的计时器，调用回调函数，利用它来执行计算或发出记录。
3. 任务根据更新后的事件时间将水位线发出。

Flink会将数据流划分为不同的分区，并将它们交由不同的算子任务来并行执行。每个分区作为一个数据流，都会包含带有时间戳的记录及水位线。

![image-20211010212124731](picture/水位线更新流程.png)

一个任务会为它的每个输入分区都维护一个**分区水位线**。当收到某个分区传来的水位线后，任务会以接收值和当前值中较大的那个去更新对应分区水位线的值。随后任务会把事件时间时钟调整为所有分区水位线中最小的那个值。如果事件时间时钟向前推动，任务会先处理因此而触发的所有计时器，之后会把对应的水位线发往所有连接的输出分区，以实现事件时间到全部下游任务的广播。

需要考虑：如果一个分区水位线不再前进的情形（声明为空闲）

当算子两个输入流的水位线相差很大，也会产生类似的情况。

### 时间戳分配和水位线生成

1. 在数据源完成分配。利用SourceFunction在应用读入数据时分配时间戳和生成水位线。如果数据源不再发出水位线，可以把自己声明为空闲的。Flink在后续算子计算水位线的时候把这些来自空闲源函数的流分区排除在外。可以解决水位线不前进的问题。
2. 周期分配器。AssignerWithPeriodicWatermarks，它从每条记录提取时间戳，并周期性地响应获取当前水位线的查询请求。
3. 定点分配器。另一个支持从记录中提取时间戳的用户自定义函数叫做AssignerWithPunctuatedWatermarks。

## 状态管理

![image-20211010213030665](picture/状态管理.png)

### 算子状态

算子状态的作用域是某个算子任务，只能在子任务之内访问到。

**列表状态**：将状态表示为一个条目列表。

**联合列表状态**：同样是将状态表示为条目列表。但是在进行故障恢复或从某个保存点启动应用时，状态的恢复方式和普通列表状态有所不同。

**广播状态**：专门为那些需要保证算子的每个任务状态都相同的场景而设计。有利于检查保存点和扩缩容。

### 键值分区状态

按照算子输入记录所定义的键值来进行维护或访问。Flink为每个键值都维护了一个状态实例，该实例总是位于那个处理对应键值记录的算子任务上。

所有键值相同的记录都能访问到一样的状态。

单值状态、列表状态、映射状态。

### 状态后端

为了保证快速访问状态，每个并行任务都会把状态维护在本地。至于状态具体的存储、访问和维护，则是由一个名为状态后端的可插拔组件来决定。

状态后端主要负责两件事：本地状态管理和将状态以检查点的形式写入远程存储。

对于本地状态管理，Flink提供了一类状态后端，会把键值分区状态作为对象，以内存数据结构的形式存在JVM堆中。另一类状态后端会把状态对象序列化后存到RocksDB中，RocksDB将它们写到本地磁盘。

状态后端负责将任务状态以检查点形式写入远程持久化存储，该远程存储可能是一个分布式文件系统。也可能是数据库。

RocksDB支持增量检查点。大规模状态会降低生成检查点的开销。

### 有状态算子的扩缩容

Flink根据不同类型的状态提供了4种扩缩容模式：

带有键值分区状态的算子在扩缩容时会根据任务的数量对键值重新分区。Flink不会对单独的键值实施再分配，而是会把所有键值分为不同的键值组。每个键值组都包含了部分键值，Flink以此为单位把键值分配给不同的任务。

带有算子列表状态的算子在扩缩容时会对列表的条目进行重新分配。所有并行算子任务的列表条目会被统一收集起来，随后均匀分配到更少或更多的任务之上。如果列表条目的数量小于算子新设置的并行度，部分任务在启动时的状态就可能为空。

带有算子联合状态的算子会在扩缩容时把状态列表的全部条目广播到全部任务上。随后由任务自己决定哪些条目该保留，哪些该丢弃。

带有算子广播状态的算子在扩缩容时会把状态拷贝到全部新任务上。

## 检查点、保存点和状态恢复

### 一致性检查点

一致性检查点的朴素机制（Flink并没有使用）：

1. 暂停接收所有的输入流
2. 等待已经流入系统的数据被完全处理，即所有任务已经处理完所有的输入数据
3. 将所有任务的状态拷贝到远程持久化存储，生成检查点。在所有任务完成自己的拷贝工作后，检查点生成完毕。
4. 恢复所有数据流的接收。

### 从一致性检查点中恢复

在流式应用执行过程中，Flink会周期性地为应用状态生成检查点。

应用恢复的3个步骤：

1. 重启整个应用
2. 利用最新的检查点重置任务状态
3. 恢复所有任务的流处理。

如果所有算子都将它们全部的状态写入检查点并从中恢复，并且所有输入流的消费位置都能重置到检查点生成的那一刻，那么该检查点和恢复机制就能为整个引用的状态提供精确一次的一致性保障。kafka运行从之前某个偏移量读取数据。

应用从检查点恢复后，它的内部状态和生成检查点的时候完全一致。随后应用就会重新消费并处理那些从之前检查点完成开始，到发生系统故障之间已经处理过的数据。（可能会有重复）但是仍可以通过重置到过去还没有处理过那些数据的时间点保证精确一次和状态一致性。

### Flink检查点算法

Flink的检查点算法基于Chandy-Lamport分布式快照算法来实现的。该算法不会暂停整个应用，而是会把生成检查点的过程和处理过程分离，这样在部分任务持久化状态过程中，其他任务还可以继续执行。

Flink检查点算法中会用到一类名为**检查点分隔符的特殊记录**。和水位线类似，这些检查点分隔符会通过数据源算子注入到常规的记录流中。相对其他记录，它们在流中的位置无法提前或延后。每个检查点分隔符都会带有一个检查点编号。这样一条数据就可以分为两部分，所有先于分隔符的记录所引起的状态更改都会被包含在分隔符多对应的检查点中，所有晚于分隔符的记录所引起的状态更改都会被纳入之后的检查点中。

<img src="picture/检查点算法.png" alt="image-20211010224510866" />

### 检查点对性能的影响

检查点会增加任务的处理延迟。Flink增加了一些策略，可以减轻某些条件下对性能的影响。

任务在将其状态写入检查点的过程中，会处于阻塞状态，此时输入会进入缓冲区。由于任务的状态可能会很大，而且生成检查点需要把这些数据通过网络写入远程存储系统，该过程可能持续的时间比较久。

Flink有状态后端生成检查点，因此任务状态的具体拷贝过程完全取决于状态后端的实现。**文件系统状态后端和RocksDB状态后端支持异步生成检查点**。

当检查点生成过程触发时，状态后端会为当期状态创建一个**本地拷贝**。在本地拷贝创建完成后，任务就可以继续它的常规处理。后台进程会异步地将本地状态快照拷贝到远程存储，然后在完成检查点后通知任务。

在分隔符对齐这一步，对于那些需要极低延迟且能容忍至少一次状态保障的应用，可以通过配置让Flink在分隔符对齐的过程中不缓冲那些已收到分隔符所对应分区的记录，而是直接处理它们。待所有的检查点分隔符都到达后，算子才将状态写入检查点，这时候状态可能会包含一些由本应出现在下一次检查点的记录所引起的改动。一旦出现故障，这些记录会被重复处理。提供的是至少一次而非精确一次。

## 保存点

Flink的故障恢复策略算法是基于状态的检查点来完成的，检查点会周期性地生成，而且会根据配置的策略自动丢弃。当应用被手动停止后，检查点也会随之删除。

保存点生成的算法与检查点完全一样，保存点的生成不是由Flink自动完成的，而是需要用户显式触发。

### 保存点的使用

可以从保存点启动应用，这样就能用保存点的数据初始化状态并从生成保存点的那一刻继续运行应用。

- 可以利用保存点修复bug
- 使用不同的并行度启动原应用。
- 迁移应用
- 暂时释放资源
- 为保存点设置不同版本并将应用状态归档。

### 从保存点启动应用

# DataStream API

## Hello Flink!



## 设置执行环境

执行环节由StreamExecutionEnvironment来表示，getExecutionEnvironment获取执行环境，能够根据调用时所处的上下文的不同，该方法可能会返回一个本地或远程环境。

```scala
val localEnv = StreamExecutionEnvironment.createLocalEnvironment // 创建本地执行环境
val remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("host", // 主机名称
                                                                   1234,  // 端口
                                                                   "patn/to/jarFile.jar") // 需要传输到JobManager的jar
env.setStreamTimeCharacteristic // 指定程序采用事件时间语义
```

注意：Flink程序都是通过延迟计算的方式执行。也就是说，那些创建数据源和转换操作的API调用不会立即触发数据处理，而只会在执行环境中构建一个执行计划。只有调用execute()方法时，系统才会触发计算。

## 转换操作

流式转换将一个或多个数据流作为输入，并将它们转换成一个或多个输出流。

完成一个Dataflow API程序的本质是通过组合不同的转换来创建一个满足应用逻辑的Dataflow

DataStream API的转换分为4类：

1. 作用于单个事件的基本转换
2. 针对相同键值事件的KeyedStream转换
3. 将多条数据流合并为一条或将一条数据流拆分为多条流的转换
4. 对流中事件进行重新组织的分发转换

### 基本转换

#### Map

![map](picture/map.png)

通过调用DataStream.map()方法可以指定map转换产生一个新的DataStream。该转换将每个到来的事件传给一个用户自定义的映射器，后者针对每个输入只会返回一个输出事件。

![MapFunction](picture/MapFunction.png)

#### Filter

![filter](picture/filter.png)

filter转换利用一个作用在流中每条输入事件上的布尔条件来决定事件的去留：如果返回true，那么它会保留输入事件并将其转发到输出。否则它会把事件丢弃。

通过调用DataStream.filter()方法可以指定filter转换产生一个数据类型不变的DataStream。

![FilterFunction](picture/FilterFunction.png)

#### FlatMap

![flatmap](picture/flatmap.png)

类似于Map，但它可以对每个输入事件产生零个、一个或多个输出事件。事实上，flatMap转换可以看做是filter和map的泛化，它实现后两者的操作。

![FlatMapFunction](picture/FlatMapFunction.png)

### 基于KeyedStream的转换

很多应用需要将事件按照某个属性分组后再进行处理。KeyedStream抽象可以从逻辑上将事件按照键值分配到多条独立的子流中。

作用于KeyedStream的状态化转换可以对当前处理时间的键值所对应的上下文中的状态进行读写。这意味着所用键值相同的事件可以访问相同的状态，因此他们可以被一并处理。

KeyedStream也支持map、flatmap和filter操作。

使用keyBy方法将DataStream转换为KeyedStream，然后对它滚动聚合以及reduce操作。

#### keyBy

![keyBy](picture/keyBy.png)

将DataStream转换为KeyedStream。流中的事件会根据各自键值被分到不同的分区，这样一来，有相同键值的事件一定会在后续算子的同一个任务上处理。虽然键值不同的事件也可能会在同一个任务上处理，但任务函数所能访问的键值分区状态始终会被约束在当前事件键值范围内。

#### 滚动聚合

滚动合操作作用于KeyedStream上，它将生成一个包含聚合结果的DataStream。滚动聚合算子会对每一个遇到过的键值保存一个聚合结果，每当有新的事件到来，该算子都会更新相应的聚合结果，并将其以事件的形式发送出去。

```scala
sum() min() max() minBy() maxBy()
// min会返回对应字段的最小值，其他字段不能保证。
// minBy会返回对应元素的所有值
```

#### Reduce

![ReduceFunction](picture/ReduceFunction.png)

reduce是滚动聚合的泛化。它将一个ReduceFunction应用在一个KeyedStream上，每个到来事件都会和reduce结果进行一次聚合，从而产生一个新的DataStream。reduce不会改变数据类型。

### 多流转换

### Union

DataStream.union()方法可以合并两条或多条类型相同的DataStream，生成一个新的类型相同的DataStream。

union执行过程中，来自两条流的事件会以FIFO的方式合并，其顺序无法得到保证。union不会对数据进行去重。

```scala
val parisStream: DataStream[SensorReading] = ...
val tokyoStream: DataStream[SensorReading] = ...
val rioStream: DataStream[SensorReading] = ...
val allCities: DataStream[SensorReading] = parisStream.union(tokyoStream, rioStream)
```

#### Connect, CoMap, CoFlatMap

在流处理中，合并两条数据流中的事件是一个非常普遍的需求。DataStream API提供的connect转换可以用来实现该需求。

ConnectedStreams提供了map()和flatmap()方法，它们分别接收一个CoMapFunction参数和CoFlatMapFunction作为参数。两个函数都是以两条输入流定义了各自的处理方法。map1和flatMap1处理第一条流的事件，map2和flatMap2处理第二条流的事件。

![connectFunction](picture/connectFunction.png)



connect()方法不会使两条输入流的事件之间产生任何关联，因此所有事件都会随机分配给算子实例。该行为会产生不确定的结果。为了在ConnectedStreams上产生确定的结果，connect可以与keyBy和broadcast结合使用。

```scala
one.connect(two).keyBy(0,0)
one.keyBy(0).connect(two.keyBy(0))
// 都会将两个流中具有相同键值的事件发往同一个算子实例

one.connect(second.broadcast)
// 所有广播流事件都会被复制多分，并分别发往后续处理函数所在你算子的每个实例。
```

CoMapFunction 和FlatMapFunction内方法的调用顺序无法控制。

### Split和Select（已经被丢弃）

split操作是union的逆操作。它将输入流分割成两条或多条类型和输入流相同的输出流。

DataStream.split()方法接收一个OutputSelector，它用来定义如何将数据流的元素分配到不同的命名输出中。OutputSelector中定义的select方法会在每个输入事件到来时被调用，并随即返回一个java.lang.Iterable[String]对象。

```scala
OutputSelector[IN]
	> select(IN): Iterable[String]
```

DataStream.split()方法会返回一个SplitStream对象，它提供了select()方法可以让我们通过指定输出名称的方式从SplitStream中选择一条或多条流。

### 分发转换

如果DataStream的并行分区存在倾斜现象，那么可能就希望通过重新平衡数据来均匀分配后续算子的负载。

控制分区策略：

- 随机：DataStream.shuffle()，该策略会均匀分布随机地将记录发往后继算子任务。

- 轮流：rebalance, 事件按照轮流方式均匀分配给后继任务。

- 重调：rescale， 会以轮流的方式对事件进行分发，但分发目标仅限于部分后继任务。当接收端任务远大于发送端任务时，该方法会更有效。

  rebalance会在所有发送任务和接受任务之间建立通信连接

  rescale每个发送任务只会和下游算子的部分任务建立连接。

- 广播 ：broadcast， 将输入流中的事件复制并发往所有下游算子的并行任务。

- 全局：global，将输入流中的所有事件发往下游算子的第一个并行任务。因为所有事件都发往同一个任务可能会影响性能。

- 自定义

  利用partitionCustom()定义分区策略，该方法接收一个Partitioner对象。

  ```scala
  object MyPartitioner extends Partitioner[Int] {
      var r = new Random()
      override def partition(key: Int, numPartitions: Int): Int = {
          if(key < 0) 0 else r.nextInt(numPartitions)
      }
  }
  numbers.partitionCustom(MyPartitioner, 0)
  ```

### 设置并行度

```scala
val defaultP = env.env.getParallelism // 获取客户端指定的并行度
env.setParallelism(32) // 设置环境并行度

val result = env.addSource(new CustomSource)
.map(new MyMapper).setParallelism(defaulP*2)
.print().setParallelism(2)
```

## 类型

出于网络传输，读写状态、检查点和保障点等目的，Flink需要对对象进行序列化和反序列化。为了提高效率，Flink有必要详细了解应用处理的数据类型。Flink利用类型信息的概念来表示数据类型，并且每种数据类型都会为其生成特定的序列化器和反序列化器以及比较器。

Flink还有一个类型提取系统，它可以通过分析函数的输入、输出类型来自动获取类型信息、继而得到相应的序列化器和反序列化器。

### 支持的数据类型

原始类型、Java和Scala元组、Scala样例类、POJO（Avro生成的类）、一些特殊类型。

那些无法特别处理的类型会被当做泛型类型交给Kryo序列化框架进行序列化。尽量避免使用Kryo，Kryo的效率通常不高。

#### 原始类型

Flink支持所有Java和Scala的原始类型。（Int Double）

#### java和scala元组

```scala
env.fromElements(("adam", 17), ("Sarah", 23))
```

java的元组是可变的，scala的元组不可变

#### scala样例类

```scala
case class Person(name: String, age: Int)
```

#### POJO

如果满足如下条件，Flink会将它看做POJO：

- 一个公有类
- 公有的无参默认构造器
- getter和setter方法
- 所有字段都必须是Flink所支持的。

Flink还会将Avro自动生成的类作为POJO处理。

#### 数组、列表、映射、枚举及其他特殊类型

### 为数据类型创建类型信息

Flink类型系统的核心类是TypeInformation，它为系统序列化器和比较器提供了必要的信息。

当应用提交执行时，Flink类型系统会为将来所需要处理的每种类型自动推断TypeInformation。一个名为类型提取器的组件会分析所有函数的泛型类型和返回类型，以获得相应的TypeInformation对象。

有时类型提取器会失灵，或者你需要定义自己的类型并告知Flink该如何高效地处理它们，这种情况下，你就需要为特定数据类型生成TypeInformation。

Flink为java和scala提供了两个辅助类，其中的静态方法可以用来生成TypeInformation。

Java中的辅助类型org.apahce.flink.api.common.typeInfo.Types

```java
TypeInformation<Integer> intType = Types.INT;
TypeInformation<Tuple2<Long, String>> tupleType = Types.TUPLE(Types.LONG, Types.STRING);
TypeInformation<Person> personType = Types.POJO(Person.class)
```

scala 辅助类型org.apache.flink.api.scala.typeutils.Types

```scala
Types.STRING
Types.TUPLE[(Int, Long)]
Types.CASE_CLASS[PERSON]
```

### 显式提供类型信息

对于部分数据类型，可能需要你向Flink显式提供TypeInformation对象。

提供TypeInformation的方法有两种。一是实现ResultTypeQueryale接口

```scala
class Tuple2ToPersonMapper extends MapFunction[(String, Int), Person] with ResultTypeQueryable[Person] {
    override def map(v: (String, Int)): Person = Person(v._1, v._2)
    // 为输出类型提供TypeInformation
    override def getProducedType: TypeInformation[Person] = Types.CASE_CLASS[Person]
}
```

方式二：使用return方法显式指定返回类型

```scala
tuples.map(t -> new Person(t.f0, t.f1))
.returns(Types.POJO(Person.class))
```

## 定义键值和引用字段

### 字段位置（@deprecated）

针对元组，可以简单地使用元组相应元素的字段位置来定义键值。

```scala
val input: DataStream[(Int, String, Long)] = ...
val keyed = input.keyBy(1) // 将第二个字段作为键值
input.keyBy(1, 2) // 将第二、三个字段作为键值
```

### 字段表达式（@deprecated)

```scala
case class SensorReading(id: String, timestamp: Long, temperature: Long)
val sensorStream: DataStream[SensorReading] = ...
sensorStream.keyBy("id")
```

Pojo和样例类也可以根据字段名称进行选择。

元组既可以利用字段名称（scala从1开始，java从0开始）也可以用从0开始的索引。

```scala
val input: DataStream[(Int, String, Long)] = ...
input.keyBy("2")
input.keyBy("_1") // 第1个字段
```

```java
DataStream<Tuple3<Integer, String, Long>> input = ...;
input.keyBy("f2") // 第3个字段
```

POJO和元组中嵌套字段，可以利用“.”来区分嵌套级别。

```scala
case class Address (address: String, zip: String, country: String)
case class Person (name:String, birthday: (Int, Int, Int), address: Address)

persons.keyBy("adddress.zip")
persons.keyBy("birthday._1")
persons.keyBy("birthday._")
```

### 键值选择器

```scala
KeySelector[IN, KEY]
	> getKey(IN): KEY
```

```scala
val sensorData: DataStream[SensorReading] = ...
val byId: KeyedStream[SensorReading, String] = sensorData.keyBy(r => r.id)
```

## 实现函数

### 函数类

Flink中所有用户自定义函数（MapFunction、FilterFunction及ProcessFunction）

通过实现接口或继承抽象类的方式实现函数。

```scala
class FlinkFilter extends FilterFunction[String] {
    override def filter(value: String): Boolean ={
        value.contains("flink")
    }
}
stream.filter(new FlinkFilter)
```

通过匿名类实现函数

```scala
stream.filter(new RichFilterFunction[String] {
    override def filter(value: String): Boolean = {
        value.contains("flink")
    }
})
```

函数必须是Java可序列化的

通过Lambda实现函数

## 富函数

DataStream API中所有的转换函数都有对应的富函数，富函数的使用位置和普通函数以及Lambda函数相同。它们可以像普通函数类一样接收参数，富函数的命名规则以Rich开头，后面跟着普通转换函数的名字。

在使用富函数的时候，你可以对应函数的生命周期实现两个额外的方法：

- open()，富函数的初始化方法。它在每个任务首次调用转换方法前一次调用。open方法通常用于哪些只需要进行一次的设置工作。Configuration参数只在DataSet API中有效。
- close() 作为函数的终止方法，会在每个任务最后一次调用转换方法后调用一次。它通常用于清理资源和释放资源。

此外可以通过getRuntimeContext方法方法RuntimeContext。可以访问函数的并行度、函数所在子任务的编号以及执行函数的任务名称。同时，还提供了访问分区状态的方法。

```scala
class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
    val subTaskIndex = 0
    override def open(configuration: Configuration): Unit = {
        subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
        // 初始化
    }
    override def flatMap(in: Int, out:Collector[(Int, Int)]): Unit=???
    override def close(): Unit =???
}
```

# 基于时间和窗口的算子

## 配置时间特性

时间特性是StreamExecutionEnvironment的一个属性，它可以接收以下值：

![TimeCharacteristic](picture/TimeCharacteristic.png)

ProcessingTime指定算子根据处理机器的系统时钟决定数据流当前的时间。

EventTime指定算子根据自身包含的信息决定当前时间。

IngestionTime指定每个接收的记录都把在数据源算子的处理时间作为事件时间的时间戳，并自动生成水位线。IngestionTime是ProcessingTime和EventTime的混合体。价值不大。

### 分配时间戳和生成水位线

![时间戳和水位线生成方式](picture/时间戳和水位线生成方式.png)

DataStream API中提供了TimestampAssigner接口，用于从已读入流式应用的元素中提取时间戳。通常情况下，应该在数据源函数后面立即调用时间戳分配器，因为大多数的分配器在生成水位线的时候都会做出一些有关元素顺序相对时间戳的假设。由于元素的读取过程通常是并行的，所以一切引起Flink跨并行数据流分区进行重新分发的操作都会导致元素的时间戳发生乱序。

最佳做法是尽可能在靠近数据源的地方，甚至是SourceFunction内部，分配时间戳并生成水位线。如果后续操作没有重分区（过滤， map），可以考虑将分配时间戳和生成水位线置后。

```scala
env.addSource(new SensorSource).assignTimestampAndWatermarks(new MyAssigner)
```

<img src="picture/TimestampAssigner.png" alt="TimestampAssigner" style="zoom:150%;" />

#### 周期性水位线分配器

周期性水位线的含义是我们会指示系统以固定的机器时间间隔来发出水位线并推动事件时间前进。默认时间间隔是200ms。

```scala 
env.getConfig.setAutoWatermarkInterval(5000) 
// 修改间隔，每5秒生成一次水位线，调用一次调用AssignerWithPeriodicWatermarks的getCurrentWatermark方法，如果该方法的返回值非空，并且大于上一个水位线的时间戳，那么算子就会发出一个新的水位线。
```

如果你确定输入的元素是单调增加的，则可以使用一个简单方法assignAscendingTimestamps

```scala
stream.assignAscendingTimestamps(e => e.timestamp)
```

另一种情况你知道输入流中的延迟（任意新到元素和已到时间戳最大元素之间的时间差）这种情况可以用BoundedOutOfOrdernessTimeStampExtractor

#### 定点水位线分配器

有时候流中会有一些用于指示系统进度的特殊元组或标记。Flink为此类情形提供了AssignerWithPunctuatedWatermarks接口。该接口中的checkAndGetNextWatermark方法会针对每个事件的extractTimestamp方法后立即调用。它可以决定是否生成一个新的水位线。如果该方法返回一个非空、且大于之前值的水位线算子就会将这个新水位线发出。

### 水位线、延迟及完整性

## 处理函数

基本转换API访问不了时间戳和水位线。

![处理函数功能](picture/处理函数功能.png)

处理函数常被用于构建事件驱动型应用，或实现一些内置窗口及转换无法实现的自定义逻辑。

Flink SQL支持的算子都是利用处理函数实现的。

![AbstractRichFunction](picture/AbstractRichFunction.png)

KeyedProcessFunction作用于KeyedStream之上。该函数会针对流中的每条记录调用一次，并返回零个、一个或多个记录。此外KeyedProcessFunction[KEY, IN, OUT]还提供了两个方法：

1. processElement(v: IN, ctx: Context, out: Collector[OUT])会针对流中的每条记录都调用一次。你可以像往常一样将结果记录传递给Collector发送出去。Context对象是让处理函数与众不同的精华所在。你可以通过它访问**时间戳、当前记录的键值以及TimerService**。此外，Context还支持将结果发送到**副输出**。
2. onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT])是一个回调函数，它会在之前注册的计时器触发时被调用。timestamp参数给出了所触发计时器的时间戳，Collector可用来发出记录。OnTimerContext能够提供和processElement方法中的Context对象相同的服务，此外，它还会返回触发计时器的时间域（处理时间还是事件时间）

### 时间服务和计时器

Context和OnTimerContext对象中的TimerService提供了以下方法：

![TimerService](picture/TimerService.png)



计时器触发时会抵用onTimer方法。系统对于processElement和onTimer方法的调用是同步的。

每个键值和时间戳只能注册一个计时器。换言之，每个键值可以有多个计时器、但具体到时间戳只有一个。默认情况下KeyedProcessFunction会将全部计时器的时间戳放到堆中的一个优先队列里，可以配置放到RocksDB。

所有计时器会和其他状态一起写入检查点。

### 向副输出发送数据

大多数DataStream算子只有一个输出，即只能生成一条某个数据类型的结果流。处理函数提供的副输出功能允许从同一个函数发出多条数据流，且它们的类型可以不同。每条副输出都由一个OutputTag[X]对象标识，其中X是副输出结果流的类型。处理函数可以利用Context对象将记录发送至一个或多个副输出。

```scala
class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading]{
    lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")
    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
        if(i.temperature < 32.0) {
            context.output(freezingAlarmOutput, s"Freezing Alarm for ${i.id}")
        }
        collector.collect(i)
    }
}
```

```scala
val monitoredReadings: DataStream[SensorReading] = readings.process(new FreezingMonitor)
// 副输出
monitoredReadings.getSlideOutput(new OutputTag[String]("freezing-alarms")).print()
// 主输出
readings.print()
```

### CoProcessFunction

针对有两个输入的底层操作，DataStream API提供了CoProcessFunction和CoFlatMapProcessFunction。

## 窗口算子

窗口可以在无限数据流上基于有界区间实现聚合等操作。通常这些区间都是基于时间逻辑定义的。

### 定义窗口算子

窗口算子可用在键值分区或非键值分区的数据流上。用在键值分区窗口的算子可以并行计算，而非键值分区窗口只能单线程处理。

新建一个窗口算子需要指定两个窗口组件：

1. 一个用于决定输入流中元素该如何划分窗口适配器。窗口适配器会产生一个WindowedStream（非键值分区的是AllWindowedStream）
2. 一个作用于WindowedStream上，用于处理分配到窗口中元素的窗口函数。

```scala
stream.keyBy(...).window(...).reduce/aggregate/process()
stream.windowAll(...).reduce/aggregate/process()
```

### 内置窗口分配器

所有内置的窗口分配器都提供了一个默认的触发器，一旦时间超过了窗口的结束时间就会触发窗口计算。窗口会随着系统首次为其分配元素而创建，Flink永远不会对空窗口执行计算。

Flink内置窗口分配器所创建的窗口类型为TimeWindow，该窗口类型实际上表示两个时间戳之间的时间区间（左闭右开）。它对外提供了获取窗口边界、检查窗口是否相交以及合并重叠窗口等方法。

#### 滚动窗口

窗口分配器会把元素放入大小固定且互不重叠的窗口中。

DataStream针对事件时间和处理时间的滚动窗口分别提供了对应的分配器**TumblingEventTimeWindows**和**TumblingProcessingTimeWindows**。

```scala
val avgTemp = sensorData.keyBy(_.id).window(TumblingEventTimeWindows.of(Time.seconds(1))).process(new TemperatureAverage)
// TumblingProcessingTimeWindows.of(Time.seconds(1)) // 对应处理时间
```

简写

```scala
val avgTemp = sensorData.keyBy(_.id).timeWindow(Time.seconds(1)).process(new TemperatureAverage)
// 具体是事件时间还是处理时间，看env的配置
```

默认情况下，滚动窗口会和纪元时间（1970....）对齐。例如，大小为1小时的分配器会在00:00:00，01:00:00触发。你可以指定一个偏移量，让它们在00:15:00，01:15:00触发。

```scala
val avgTemp = sensorData.keyBy(_.id).window(TumblingEventTimeWindows.of(Time.hours(1), Time.mimutes(15))).process(new TemperatureAverage)
```

#### 滑动窗口

滑动窗口分配器将元素分配给大小固定且按指定滑动间隔移动的窗口。

对于滑动窗口，你需要指定窗口大小以及用于定义新窗口开始频率的滑动间隔。如果滑动间隔小于窗口大小，则窗口会出现重叠。如果滑动间隔大于窗口大小，则一些元素可能不会分配给任何窗口。

```scala
val slidingAngTemp = sensorData.keyBy(_.id).window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15))).process(...)
// SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(15))
```

简写

```scala
val avgTemp = sensorData.keyBy(_.id).timeWindow(Time.hours(1), Time.minutes(15)).process(new TemperatureAverage)
```

#### 会话窗口

会话窗口将元素放在长度可变且不重叠的窗口中。会话窗口的边界由非活动间隔，及持续没有收到记录的时间间隔来定义。

```scala
val sessionWindows = sensorData.keyBy(_.id).window(EventTimeSessionWindows.withGrap(Time.minutes(5))).process(...)
// ProcessingTimeSessionWindows.withGrap(Time.minutes(5))
```

### 在窗口上应用函数

增量聚合函数。 它的应用场景是窗口内以状态形式存储某个值且需要根据加入窗口的元素对该值进行更新。此类**函数十分节省空间**且最终会把聚合值发送出去。ReduceFunction和AggregateFunction属于增量聚合函数

全量窗口函数。它将收集窗口内的所有元素，并在执行计算时对它们进行遍历。占用更多的空间，但它和增量聚合函数相比，支持更复杂的逻辑。ProcessWindowFunction是一个全量窗口函数。

#### ReduceFunction

接收两个类型相同的值，并把他们组合成一个类型不变的值。当被用在窗口化数据流上时，ReduceFunction会对分配给窗口的元素进行增量聚合，窗口只需要存储增量聚合的结果。

#### AggregateFunction

AggregateFunction会以增量的方式应用于窗口内的元素。此外，使用了AggregateFunction的窗口算子，其状态只有一个值。

```java
public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable{
    // 创建一个累加器来启动集合
    ACC createAccumulator();
    // 向累加器中添加一个输入元素并返回累加器
    ACC add(IN value, ACC accumulator);
    // 根据累加器计算并返回结果
    OUT getResult(ACC accumulator);
    // 合并两个累加器并返回结果
    ACC merge(ACC a, ACC b);
}
```

#### ProcessWindowFunction

```java
public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> extends AbstractRichFunction {
    // 对窗口执行计算
    void process(KEY key, Context ctx, Iterable<IN> vals, Collector<OUT> out);
    // 在窗口清除时删除自定义的单个窗口状态
    public void clear(Context contxt) throws Exception{}
    // 保存窗口元数据的上下文
    public abstract class Context implements Serializable {
        // 返回窗口的元数据
        public abstract W window();
        // 返回当前处理时间
        public abstract long currentProcessingTime();
        // 返回当前事件时间水位线
        public abstract long currentWatermark();
        // 用于单个窗口状态的访问器
        public abstract KeyedStateStore windowState();
        // 用于每个键值全局状态的访问器
        public abstract KeyedStateStore globalState();
        // 向OutputTag标识的副输出发送记录
        public abstract <X> void output(OutputTag<X> outputTag, X value);
    }
}
```

在内部，ProcessWindowFunction处理的窗口会将所有已分配的事件存储在ListState中。

#### 增量聚合函数与ProcessWindowFunction

其实很多情况下由于窗口的逻辑都可以表示为增量聚合，只不过还需要访问窗口的元数据或状态。

如果可能增量聚合表示逻辑还需要访问窗口元数据，则可以将ReduceFunction或AggregateFunction与功能更强的ProcessWindowFunction组合使用。你可以对分配给窗口人都函数立即执行聚合，随后当窗口触发时，再将聚合的结果发送给ProcessWindowFunction。这样传递给ProcessWindowFunction.process方法的Iterable参数内将只有一个值。

在DataStream API中，实现上述过程的途径是将ProcessWindowFunction作为reduce()或aggregate()方法的第二个参数。

```scala
input.keyBy(...).timeWindow(...).reduce(incrAggregator: ReduceFunction[IN],function: ProcessWindowFunction[IN, OUT, K, W])
input.keyBy(...).timeWindow(...).aggregate(incrAggregator: ReduceFunction[IN],function: ProcessWindowFunction[IN, OUT, K, W])
```

### 自定义窗口算子

DataStream API对外暴露了自定义窗口算子的接口和方法。你可以实现自己的**分配器、触发器以及移除器**。

当一个元素进入窗口算子时会被移交给**WindowAssigner**。该分配器决定了元素应该被放入哪几个窗口中。如果目标窗口不存在，则会创建它。

如果为窗口算子配置的是增量聚合函数，那么新加入的元素会立即执行聚合，其结果会作为窗口内容而存储。如果窗口算子没有配置增量聚合函数，那么新加入的元素会附加到一个用于存储所有窗口分配元素的ListState上。

每个元素在加入窗口后还会被传递至该窗口的触发器。触发器定义了窗口何时准备好执行计算，何时需要清除自身及保存的内容。触发器可以根据已分配的元素或注册的计时器来决定在某特定时刻执行计算或清除窗口中的内容。

触发器成功触发后的行为取决于窗口算子所配置的函数。如果算子只是配置了一个增量聚合函数，就会发出当前聚合结果。

![配置了增量聚合函数的窗口算子](picture/配置了增量聚合函数的窗口算子.png)

如果算子只包含了一个全量窗口函数，那么该函数将一次性作用于窗口内的所有元素上，之后便会把结果发出。

![全量窗口的算子函数](picture/全量窗口的算子函数.png)

如果配置了增量聚合函数和全量窗口函数，那么后者将作用于前者产生的聚合值上，之后便会把结果发出。

![增量聚合及全量窗口函数的窗口算子](picture/增量聚合及全量窗口函数的窗口算子.png)

移除器作为一个可选组件，允许在ProcessWindowFunction调用之前或之后引入。它可以用来从窗口中删除已经收集的元素。由于需要遍历所有元素，移除器只有在未指定增量聚合函数的时候才能使用。

```scala
stream.keyBy(...)
.window(...)
.triger(...)  // 触发器 
.evictor(...) // 移除器
.reduce/aggregate/process(...)
```

移除器并非可选组件，但每个窗口算子都要有一个触发器来决定何时对窗口进行计算。为了使窗口算子的API保持简洁，系统对于没有显示指定触发器的WindowAssigner都会提供一个默认的触发器。

### 窗口的生命周期

窗口会在WindowAssigner首次向它分配元素时创建，因此每个窗口至少有一个元素。

- 窗口内容：窗口内容包含了分配给窗口的元素，或当窗口算子配置了ReduceFunction或AggregateFunction时增量聚合所得到的结果。

- 窗口对象：WindowAssigner会返回0个、1个或多个窗口对象。窗口算子会根据返回的对象对元素进行分组。因此窗口对象中保存着用于区分窗口的信息。每个窗口对象都有一个结束时间戳，它定义了可以安全删除窗口及其状态的时间点。

- 触发器计时器：可以在触发器中注册计时器，用于在将来某个时间点触发回调（对内容计算或者清除），这些计时器由窗口算子负责维护。

- 触发器中的自定义状态：触发器中可以定义和使用针对每个窗口、每个键值的自定义状态。该状态并非由窗口算子进行维护，而是完全由触发器来控制。

窗口算子会在你窗口结束时间到达时删除窗口。该时间是处理时间还是事件时间语义取决于WindowAssigner.isEventTime()方法的返回值。

当窗口需要删除时，窗口算子会自动清除窗口内容，并丢弃窗口对象。自定义触发器状态和触发器中注册的计时器将不会被清除，因为这些状态对于窗口算子而言是不可见的。所以说为了避免状态泄漏，触发器需要在Trigger.clear方法中清除自身所有状态。

### 窗口分配器

WindowAssigner用于决定将到来的元素分配给哪些窗口。每个元素可以被加到零个、1个或多个窗口中。

```java
public abstract class WindowAssigner<T, W extends Window> implements Serialiazable {
    // 返回元素分配的目标窗口集合
    public abstract Collection<W> assignWindows(T element, long timestamp, WinidowAssignerContext context);
    // 返回WindowAssigner的默认触发器
    public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);
    // 返回WindowAssigner中窗口的TypeSerializer
    public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);
    // 表明此分配器是否创建基于事件时间的窗口
    public abstract boolean isEventTime();
    // 用于访问当前处理时间的上下文
    public abstract static class WindowAssignerContext{
        public abstract long getCurrentProcessingTime();
    }
}
```

 GlobalWindows分配器

将所有元素都分配到一个窗口中，默认是NeverTrigger。所以需要自定义的触发器。

MergingWindowAssigner可用于需要对已有窗口进行合并的窗口算子。

### 触发器

触发器用于定义何时对窗口进行计算并发出结果。它的触发条件可以是时间，也可以是某些特定的数据条件。之前讨论的时间窗口而言，其默认触发器会在处理时间或水位线超过了窗口结束边界的时间戳时触发。

触发器不仅能够访问时间属性和计时器，还可以使用状态，因此它在某种意义上等价于触发函数。

触发逻辑：窗口收到一定量的元素、窗口含有某个特定值的函数、添加的元素满足某种模式

自定义触发器还可以在水位线到达窗口的结束时间戳以前，为事件时间窗口计算并发出早期结果。这是一个在保守的水位线策略下依然可以产生低延迟的结果的方法（非完整）。

每次调用触发器都会生成一个TriggerResult，它决定窗口接下来的行为。

```scala
CONTINUE // 什么都不做
FIRE // 如果窗口算子配置了ProcessWindowFunction，就会调用该函数并发出结果。如果窗口只包含一个增量聚合函数（ReduceFunction或AggregateFunction），则直接发出结果。窗口的状态不会发生变化
PURGE // 完全清除窗口的内容，并删除窗口自身机器元数据。同时调用ProcessWindowFunction.clear()方法来清理那些自定义的单个窗口状态。
FIRE_AND_PURGE // 先计算，后清除
```

```java
public abstract class Trigger<T, W extends Window> implements Serializable {
    // 每当有元素添加到窗口时都会调用
    TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx);
    // 在处理时间计时器触发时调用
    public abstract TriggerResult onProcessingTime(long timestamp, W window, TriggerContext ctx);
    // 在事件时间计时器触发时调用
    public abstract TriggerResult onEventTime(long timestamp, W window, TriggerContext ctx);
    // 如果触发器支持合并触发器状态则返回true
    public boolean canMerge();
    // 当多个窗口合并为一个窗口
    // 且需要合并触发器状态时调用
    public void onMerge(W window, OnMergeContext ctx);
    // 在触发器中清除那些为给定窗口保存的状态
    // 该方法会在清除窗口时调用
    public abstract void clear(W window, TriggerContext ctx);
}
```

```java
public interface TriggerContext {
    long getCurrentProcessingTime();
    long getCurrentWatermark();
    void registerProcessingTimeTimer(long time);
    void registerEventTimeTimer(long time);
    void deleteProcessingTimeTimer(long time);
    void deleteEventTimeTimer(long time);
    <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);
}

public interface OnMergeContext extends TriggerContext {
    void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor);
}
```

当在触发器中使用单个窗口状态时，你需要保证它们会随着窗口删除而被正确地清理。否则窗口算子的状态会越积越多，最终可能会导致你的应用在某个时间出现故障。为了在删除窗口时彻底清理状态，触发器的clear()方法需要删除全部自定义的窗口状态并使用TriggerContext对象删除所有处理时间和事件时间计时器。

如果某个触发器和MergingWindowAssigner一起使用，则需要处理两个窗口合并的情况。canMerge声明了某个触发器支持合并，而相应地需要onMerge方法实现合并逻辑。

在合并触发器时，需要把所有自定义状态的描述符传递给OnMergeContext对象的mergePartitionedState方法。

### 移除器

Evictor是Flink窗口机制的一个可选组件，可以用于窗口执行计算前或后从窗口删除元素。

```scala
public interface Evictor<T, W extends Window> extends Serializable {

	/**
	 * Optionally evicts elements. Called before windowing function.
	 *
	 * @param elements The elements currently in the pane.
	 * @param size The current number of elements in the pane.
	 * @param window The {@link Window}
	 * @param evictorContext The context for the Evictor
     */
	void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

	/**
	 * Optionally evicts elements. Called after windowing function.
	 *
	 * @param elements The elements currently in the pane.
	 * @param size The current number of elements in the pane.
	 * @param window The {@link Window}
	 * @param evictorContext The context for the Evictor
	 */
	void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);


	/**
	 * A context object that is given to {@link Evictor} methods.
	 */
	interface EvictorContext {

		/**
		 * Returns the current processing time.
		 */
		long getCurrentProcessingTime();

		/**
		 * Returns the metric group for this {@link Evictor}. This is the same metric
		 * group that would be returned from {@link RuntimeContext#getMetricGroup()} in a user
		 * function.
		 *
		 * <p>You must not call methods that create metric objects
		 * (such as {@link MetricGroup#counter(int)} multiple times but instead call once
		 * and store the metric object in a field.
		 */
		MetricGroup getMetricGroup();

		/**
		 * Returns the current watermark time.
		 */
		long getCurrentWatermark();
	}
}
```

移除器常用于GlobalWindow

## 基于时间的双流Join

数据流操作的另一个常见需求是对两条数据流中的事件进行联结（Join）。Flink内置有两个可以根据时间条件对数据流进行Join的算子：基于间隔的Join和基于窗口的Join。

如果内置的Join算子无法表达所需的Join语义，可以通过CoProcessFunction、BroadcastProcessFunction或KeyedBroadcastProcessFunction实现自定义的Join。

### 基于间隔的Join

基于间隔的Join会对两条流中拥有相同键值以及彼此之间时间戳不超过某一指定间隔的事件进行Join。基于间隔的Join目前只支持事件时间以及INNER_JOIN语义。

```scala
input1.keyBy(...).between(<lower-bound>, <upper-bound>).process(ProcessJoinFunction)
```

Join成功的事件会发送给ProcessJoinFunction。下界和上界分别由负时间间隔和正时间间隔来定义between(Time.hour(-1), Time.minute(15))

基于间隔的Join需要同时对双流记录进行缓冲。如果两条流中的事件时间不同步，那么Join所需的存储会显著增加，因为水位线总是由“较慢”的那条流来决定。

### 基于窗口的Join

基于窗口的Join需要用到Flink中窗口机制。其原理是将两条流中的元素分配到公共窗口并在窗口完成时进行join

```scala
input1.join(input2)
.where(...)
.equalTo(...)
.window(...)
.trigger()
.evictor()
.apply()
```

两条输入流都会根据各自的键值属性进行分区，公共窗口分配器会将两者的时间映射到公共的窗口内。当窗口的计时器出发时，算子会遍历两个输入中元素的每个组合（叉乘积）去调用JoinFunction

除了对窗口中两条流进行Join，你还可以对它们进行CoGroup，只需将join算子换位coGroup

## 处理迟到数据

### 丢弃迟到事件

窗口的默认行为

### 重定向迟到事件

利用副输出，将迟到事件重定向到另一个DataStream，这样就可以对它们进行后续处理或利用常规的数据汇函数将其写出。

迟到的数据可以通过定期的回填操作集成到应用的结果中。

### 基于迟到事件更新结果

对不完整的结果进行重新计算并发出更新。

支持重新计算会和对已发出结果进行更新的算子需要保留那些用于再次计算结果的状态。而且通常算子无法永久保留所有状态，最终还是需要在某个时间点将其清除。一旦清除了针对特定结果的状态，这些结果就再也无法更新，而迟到事件也只能被丢弃或重定向。

除了在算子中保持状态，受结果更新影响下游算子或外部系统还得能够处理这些更新。

窗口算子API提供了一个方法，可以用来显式声明支持迟到的元素。在使用事件时间窗口时，你可以指定一个名为延迟容忍度的额外事件段。配置了该属性的窗口算子在水位线超过窗口的结束时间戳或不会立即删除窗口，而是会将窗口继续保留该延迟容忍度的时间。

# 有状态的算子和应用

## 实现有状态的函数

函数的状态类型：键值分区状态和算子状态

### 在RuntimeContext中声明键值分区状态

用户函数可以使用键值分区状态和访问当前键值上下文中的状态。对于每一个键值，Flink都会维护一个状态实例。函数的键值分区状态实例会分布在函数所在算子的所有并行任务上。这意味着每个函数的并行实例都会负责一部分键值域并维护相应的状态实例。

键值分区状态看上去就像一个分布式键值映射。

键值分区状态只能由作用在KeyedStream上面的函数使用。你可以通过在一条数据流上调用定义键值的DataStream.keyBy()方法来得到一个KeyedStream。

![状态原语](picture/状态原语.png)

所有状态原语都支持State.clear()方法来进行清除。

如果是ReducingState和AggregateState，还需要在状态描述符中加入ReduceFunction和AggregateFunction对象。

如果正有状态从某检查点恢复或从某保存点重启，那么当函数注册StateDescriptor时，Flink会检查状态后端是否存储了函数相关数据以及与给定名称、类型匹配的状态。

Scala DataStream API只有单个ValueState的map和flatMap等函数提供了更为简洁的写法。比如flatMapWithState

### 通过ListCheckpointed接口实现算子列表状态

算子状态的维护是按照每个算子并行实例来分配的。因此同一算子并行任务在处理任何事件时都可以访问相同的状态。

Flink支持三种算子状态：列表状态、联合列表状态和广播状态

若要在函数中使用算子列表状态，需要实现ListCheckPointed接口。该接口不像ValueState或ListState那样直接在状态后端注册，而是需要将算子状态实现为成员变量并通过接口提供的回调函数与状态后端进行交互。

ListCheckpointed提供了两个方法

```scala
// 以列表形式返回一个函数状态的快照
snapshotState(checkpointId: Long, timestamp: Long): java.util.List[T]
// 根据提供的列表恢复函数的状态
restoreState(java.util.List[T] state): Unit
```

snapshotState会在Flink触发为有状态函数生成检查点时调用。checkpointId唯一且单调递增的检查点编号，timestamp：JobManager开始创建检查点的机器时间

restoreState初始化函数状态时调用，该过程可能发生在作业启动或故障恢复的情况下。

ListCheckpointed使用的是Java序列化机制，如果为了确保算子状态日后支持更新使用CheckpointFunction接口替换ListCheckpointed

### 使用联结的广播状态

流式应用的一个常见需求是将相同信息发送到函数的所有并行实例上，并将它们作为可恢复的状态进行保护。典型：**规则流和应用这些规则的事件流**。规则应用函数会接收这两条输入流，它会将规则存为算子状态然后将它们应用到事件流的全部事件上。规则流需要以广播的形式发送，以便每个实例都能收到全部规则。

在Flink中这种状态称为广播状态，它可以和常规的DataStream或KeyedStream结合使用。

在两条流中应用广播状态函数需要3个步骤：

1. 调用DataStream.broadcast()方法创建一个BroadcastStream并提供一个或多个MapStateDescriptor对象，每个描述符都会为将来用于BroadcastStream的函数定义一个单独的广播状态。
2. 将BroadcastStream和一个DataStream或KeyedStream联结起来。必须将BroadcastStream作为参数传给conncet方法
3. 在联结后的数据流上应用一个函数。根据另一条流是否已经按键值分区，该函数可能是KeyedBroadcastProcessFunction或BroadcastProcessFunction。

processElement传入的上下文是只读上下文，用于获取广播状态。processBroadcastElement传入的是可读写的上下文。

## 使用CheckpointedFunction接口

CheckpointedFunction是用于指定有状态函数的最底层接口。它提供了用于注册和维护键值分区状态以及算子状态的钩子函数，同时也是唯一支持使用算子联合列表状态（UnionListState，该状态在恢复时需要被完整地复制到每个任务实例上）的接口。

CheckpointedFunction接口定义了两个方法：initializeState()和snapshotState()。initializeState方法在创建CheckpointedFunction的并行实例时被调用。其触发时机是应用启动或由于故障而重启。Flink在调用该方法时会传入一个FunctionInitializationContext对象，我们可以利用它访问OperatorStateStore及KeyedStateStore对象。这两个状态存储对象能够使用Flink运行时来注册函数状态并返回状态对象（ValueState等）。我们在注册一个状态时，都要提供一个函数范围内的唯一名称。在函数注册过程中，状态存储首先会利用给定名称检查状态后端中是否存在一个为当前函数注册过的同名状态，并尝试用它对状态进行初始化。如果重启任务的情况，Flink就会用保存的数据初始化状态；如果应用不是从检查点或保存点恢复，那状态初始化为空。

snapshotState方法会在生成检查点前调用，它需要接收一个FunctionSnapshotContext对象作为参数，从FunctionSnapshotContext中，我们可以获取检查点编号以及JobManager在初始化检查点时的时间戳。snapshotState()方法的目的是确保检查点开始之前所有状态对象都已更新完毕。此外，该方法还可以结合CheckpointedListener接口使用，在检查点同步阶段将数据一致性地写入外部存储中。

##### 接收检查点完成通知

频繁地同步是分布式系统产生性能瓶颈的主要原因。Flink的设计旨在减少同步点的数量，其内部的检查点时基于和数据一起流动的分隔符来实现的，因此可以避免对应用所有算子实施全局同步。

在所有算子任务都成功将其状态写入检查点存储后，整体的检查点才算创建成功。因此，只有JobManager才能对此做出判断。算子为了感知检查点创建成功，可以实现CheckpointListener接口。该接口提供的notifyCheckpointComplete(long checkpointId)方法，会在JobManager将检查点注册为已完成时被调用。

## 为有状态的应用开启故障恢复

```scala
// 显式为应用开启检查点功能
env.enableCheckpointing(10000L) // 检查点生成周期是10s
```

较短的间隔会为常规应用带来较大的开销，但由于恢复时要重新处理的数据量较小，所以恢复速度更快。

Flink为检查点行为提供了其他一些可供调节的配置项，例如，一致性保障（精确一次或至少一次的选择），可同时生存的检查点数目以及用来取消长时间运行检查点的超时时间，以及多个和状态后端相关的选项。

## 确保有状态应用的可维护性

应用运行一段时间后，其状态维护变得成本十分昂贵，甚至无法重新计算。需要对长时间运行的应用进行一些维护。

Flink利用保存点机制来对应用及其状态进行维护，但它需要初始版本应用的全部有状态算子都制定好两个参数，才可以在未来正常工作。这个两个标识是**算子唯一标识以及最大并行度**。

##### 指定算子的唯一标识

你应该为应用中的每个算子指定唯一标识。该标识作为元数据和算子的实际状态一起写入保存点。当应用从保存点启动时，会利用这些标识将保存点中的状态映射到目标应用对应的算子。

如果没有为有状态的算子显式指定标识，那么在更新应用是就会收到诸多限制。

```scala
val alerts: DataStream[(String, Double, Double)] = keyedSensorData.flatMap(new TemperatureAlertFunction(1.1)).uid("TempAlert")
```

### 为使用键值分区状态的算子定义最大并行度

算子的最大并行度参数定义了算子在对键值状态进行分割时，所能用到的键值组数量。该数量限制了键值分区状态可以被扩展到最大并行任务数。

```scala
// 设置应用的最大并行度
env.setMaxParallelism(512) 
val alerts: DataStream[(String, Double, Double)] = keyedSensorData.
flatMap(new TemperatureAlertFunction(1.1)).setMaxParallelism(1024) //算子最大并度
```

算子默认最大并行度会取决于应用首个版本中算子的并行度：

- 如果并行度小于128， 则最大并行度设置为128
- 如果算子的并行度大于128，会取(parallelism+(parallelism/2)) 和2^15之中的较小值。

## 有状态应用的性能及鲁棒性

算子和状态的交互会对应用的鲁棒性及性能产生一定的影响。

### 选择状态后端

状态后端负责存储每个状态实例的本地实例，并在生成检查点时将它们写入远程持久化存储。

状态后端被设计为“可插拔的”，每个状态后端都为不同的状态原语提供了不同实现。

Flink提供了三种状态后端：MemoryStateBackend, FsStateBackend, RocksDBStatedBackend

![状态后端](picture/状态后端.png)

```scala
// 远程文件系统检查点路径配置
val backend = new RocksDBStateBackend(checkpointPath)
env.setStateBackend(backend)
```

### 选择状态原语

对于设计对象序列化和反序列化的状态后端（RockDBStateBackend），状态原语的选择将对应用的性能产生决定性的影响。

ValueState在更新和访问时分别进行完整的序列化和反序列化。

在构造Iterable对象前，RocksDBStateBackend的ListState需要将它所有列表条目反序列化。但向ListState添加一个值的操作会相对轻量级，只会序列新添加的值。

MapState允许按照每个键对其数据进行读写，并且只有那些读写的键和数据值才需要进行反序列化。在遍历MapState的条目时，状态后端会从RocksDB中预取出序列化好的所有条目，并只有在实际访问某个键或数据值的时候才会将其反序列化。

使用MapState[X, Y]会比ValueState[HashMap[X, Y]]更高效。如果经常在列表后面添加元素摈弃列表元素的访问频率很低，那么ListState[X]会比ValueState[List[X]]好

### 防止状态泄漏

状态很大可能会杀死进程。为了防止资源耗尽，需要控制状态的大小。

Flink无法通过清理状态来释放资源。

导致状态增长的一个常见原因是键值状态的键值域不断发生变化。如果一些键值在一段时间不在使用，可以被清理掉。

### 更新有状态的应用

```
生成保存点-> 停止应用 -> 重启新版本
```

原始应用和新版本应用的检查点必须兼容。从保存点的角度来看，应用可以通过三种方式进行更新：

1. 在不对已有状态进行更改或删除前提下更新或扩展应用逻辑，包括向应用添加有状态或无状态算子。
2. 从应用中移除某个状态
3. 通过改变状态原语或数据类型来修改已有算子的状态

#### 保持现有状态更新应用

如果应用在更新时不会删除或者改变已有状态，那么它一定是保存点兼容的，并且能够从旧版本的保存点启动。

如果你向应用中添加了新的有状态算子或为已有算子增加了状态，那么应用从保存点中启动时，这些状态会被初始化为空。

#### 从应用中删除状态

除了向应用中添加状态，你可能还想在修改应用的同时从中删除一些状态。这些状态操作所针对的可以是一个完整的有状态算子，也可以是函数中的某个状态。当新版本的应用从旧版本的保存点启动时，保存点中的部分状态将无法映射到重启的应用中。

为了避免保存点中的状态丢失，Flink在默认情况下不允许那些无法将保存点中的状态全部恢复的应用启动。但是可以禁用这一安全检查。

#### 修改算子的状态

有两种办法对状态修改：

1. 通过更改状态的数据类型，例如将ValueState[Int]改为ValueState[Double]
2. 修改状态的原语类型。ValueState[List[String]] 改为ListState[String]。目前不支持

### 可查询状态

Apache Flink提供了可查询式状态功能。在Flink中，任何键值分区状态都可以作为可查询式状态暴露给外部应用，就像一个只读的键值存储一样。有状态的流式应用可以按照正常流程处理事件，并在可查询状态中对其中间或最终结果进行存储和更新。

可查询式状态无法应对所有需要外部数据存储的场景。原因之一：它只有在应用运行期间才可以访问。如果应用正在因为错误而重启、正在进行扩缩容、或正在迁移至其他集群，那么可查询式状态无法访问。

### 可查询式状态服务的架构及启动方式

QueryableStateClient用于外部系统提交查询及获取结果

QueryableStateClientProxy用于接收并响应客户端请求。该客户端代理需要在每个TaskManager上面都运行一个实例。

QueryableStateServer用于处理客户端代理的请求。在TaskManager上运行。

在Flink中启用可查询式状态服务，你需要将flink-queryable-state-runtime JAR文件放到TaskManager进程的classpath下。这样可查询式状态的线程就会自动启动。

### 对外暴露可查询式状态

```scala
val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
// 启用可查询式状态并设置其外部标识符
lastTempDescriptor.setQueryable("lastTemperature")

// 支持利用数据汇将流中所有事件都存到可查询式状态中。
tenSecsMaxTemps.keyBy(_._1).asQueryableState("maxTemperature")
```

asQueryableState方法会添加一个可查询式状态的数据汇。可查询状态的类型是ValueState，它内部的数据类型和输入流的类型相同。

### 从外部系统查询状态

```xml
<dependency>
    <groupid>org.apache.flink</groupid>
    <artifactid>flink-queryable-state-client-java_2.12</artifactid>
    <version>1.7.1</version>
</dependency>
```

```scala
def main(args: Array[String]): Unit = {
    val client = new QueryableStateClient(proxyHost, proxyPort)
}
```

# 读写外部系统

## 应用的一致性保障

应用的数据源和数据汇连接器能和Flink的检查点及恢复策略集成。

为了在应用中实现精确一次的状态一致性保障，应用的每个数据源连接器都需要支持将读取位置重置为某个已有检查点中的值。在生成检查点时，数据源算子会将读取位置持久化并在故障恢复过程中将其还原。支持将读取位置写入检查点的数据源连接器有：基于文件的连接器以及Kafka连接器。

Flink的检查点和恢复机制结合可重置的数据源连接器能够确保应用不会丢失数据。但由于前一次成功的检查点后发出的数据会被再次发送，所以应用可能会发出两次结果。不能提供精确一次保障。

如果要提供精确一次保障：幂等性写，事务性写。

## 内置连接器

### Kafka数据源连接器

Kafka连接器会以并行方式获取事件流。每个并行数据源任务都可以从一个或多个分区读取数据。

```scala
val props = new Properties()
props.setProperties("bootstrap.servers", "localhost")
props.setProperties("group.id", "test")

val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), props))
```

为了提取事件时间的时间戳生成水位线，你可以通过调用FlinkKafkaConsumer.assignTimestampsAndWatermark()向Kafka消费者提供一个AssignerWithPeriodicWatermark或AssignWithPunctuatedWatermark。

Kafka从版本0.10.0开始支持消息时间戳。因此当我们从0.10之后的版本读取数据时，如果应用运行在事件时间模式下，消费者会自动提取消息的时间戳作为事件时间的时间戳。仍然需要生成水位线，利用AssignerWithPeriodicWatermark和AssignWithPunctuatedWatermark转发时间戳。

```scala
FlinkKafkaConsumer.setStartFromGroupOffsets()
FlinkKafkaConsumer.setStartFromEarliest()
FlinkKafkaConsumer.setStartFromLatest()
FlinkKafkaConsumer.setStartFromTimestamp(long)
FlinkKafkaConsumer.setStartFromSpecificOffsets(Map)
```

Flink kafka Consumer默认关闭了正则匹配主题的方式，可以通过配置项flink.partition-discovery.interval-millis

### Kafka数据汇连接器

```scala
val myProducer = new FlinkKafkaProducer[String]("localhost:9092", "topic", new SimpleStringSchema)
stream.addSink(myProducer)
```

### kafka数据汇至少一次保障

在满足以下条件时，数据汇才可以提供精确一次保障：

- Flink的检查点功能处于开启状态，应用所有的数据源都是可重置的。
- 如果数据汇写入不成功，则会抛出异常，继而导致应用失败并进行故障恢复。这也是默认行为。可以配置Producer重试次数retries。也可以在数据汇对象上调用setLogFailuresOnly(true)
- 数据汇连接器要在检查点完成前等待Kafka确认记录写入完毕。这是默认行为。可以通过setFlushOnCheckpoint(false)关闭。

### Kafka数据汇的精确一次保障

FlinkKafkaProducer还提供了一个带有Semantic参数的构造函数，用来控制数据汇提供的一致性保障。

```scala
Semantic.NONE 不做一次性保障
Semantic.AT_LEAST_ONCE 保证数据不会丢失
Semantic.EXACTLY_ONCE 基于kafka事务机制，精准一次
```

transaction.timeout.ms为1小时。需要调整transaction.max.timeout.ms为15分钟

### 自定义分区和写入消息时间戳

FlinkKafkaPartitioner

## 文件系统数据源连接器

```scala
val lineReader = new TextInputFormat(null)
env.readFile[String](
	lineReader,
    "hdfs:///path/to/data",
    FileProcessingMode.PROCESS_CONTINUOUSLY,
    30000L 
)
```

![FileInputFormat](picture/FileInputFormat.png)

![FileProcessingMode](picture/FileProcessingMode.png)

如果FileInputFormat没有实现CheckpointableInputFormat，在检查点的情况下只能提供至少一次保障。



## 文件系统数据汇连接器

```scala
val sink: StreamingFileSink[String] = StreamingFileSink
.forRowFormat(
	new Path("/base/path"),
    new SimpleStringEncoder[String]("UTF-8"))
.build()
input.addSink(sink)
```

StreamingFileSink在接收到记录后。会将它分配到一个桶中。每个桶都代表了由StreamingFileSink构建器配置的基础路径的一个子目录下（"/base/path"）。

桶的选择由一个BucketAssigner来完成。它决定每条记录的bucketId。可以通过配置withBucketAssigner方法配置BucketAssigner。默认使用DataTimeBucketAssigner。

文件路径：[base-path]/[bucket-path]/part-[task-idx]-[id]

RollingPolicy用来决定何时创建一个新的分块文件。可以通过withRollingPolicy配置。默认使用DefaultRollingPolicy，它会在现有文件大小超过128M或打开时间超过60s时创建一个新的分块文件。

```scala
val sink: StreamingFileSink[String] = StreamingFileSink.
forBulkFormat(
	new Path("/base/path"),
    ParquetAvroWriters.forSpecificRecord(classOf[AvroPojo])
)
input.addSink(sink)
```

## 实现自定义数据源函数

1. SourceFunction和RichSourceFunction可用于定义非并行的数据源连接器。
2. ParallelSourceFunction和RichParallelSourceFunction可用于定义能同时运行多个任务实例的数据源连接器。

### 可重置的数据源函数

需要集成Flink检查点机制。CheckpointedFunction接口。

### 数据源函数、时间戳及水位线

## 实现自定义的数据汇函数

SinkFunction和RichSinkFunction。

## 异步访问外部系统

AsyncFunction可以有效降低I/O调用所带来的的延迟。

# 部署模式

## Yarn

Flink能够以两种模式和Yarn进行集成：作业模式和会话模式。

在作业模式下，Flink集群启动后只会运行单个作业，一旦作业结束，集群就停止，全部资源会归还。

![作业模式](picture/作业模式.png)

在会话模式下，Flink会连接到Yarn的ResourceManager来启动包含一个Dispatcher线程和一个ResourceManager线程的ApplicationManager。

![会话模式](picture/会话模式.png)

当接收到一个提交执行的作业时，Dispatcher会启动一个JobManager线程，负责从Flink的ResourceManager申请处理槽。如果处理槽的数量不足，Flink的ResourceManager会向Yarn的ResourceManager申请跟多的容器来启动TaskManager进程。

![会话模式提交作业](picture/会话模式提交作业.png)

无论是作业模式还是会话模式，Flink的ResourceManager都会自动对故障的TaskManager进行重启。可以配置Flink在yarn上的故障恢复行为。

```shell
# 作业模式
./bin/flink run -m yarn-cluster ./path/to/job.jar
# 启动会话 -d分离式启动，允许利用Yarn的工具终止会话
./bin/yarn-session.sh 
./bin/flink run # 提交作业
```

## 高可用设置

### Yarn高可用设置

Yarn作为一个集群资源和容器的管理器。默认情况下，它会自动重启发生故障的主进程容器和TaskManager容器。

```xml
<!-- yarn-site.xml -->
<property>
    <name>yarn.resourcemanager.am.max-attempts</name>
    <value>4</value>
    <description>
    	ApplicationMaster尝试执行的最大次数。默认是2，即应用最多重启一次
    </description>
</property>
```

yarn只会计算因为应用故障导致重启的次数，而由抢占、硬件故障或机器重启等因素导致的重启将不会算在应用尝试次数内。

# Flink流式应用运维

## 通过命令行客户端管理应用

```bash
# 启动
./bin/flink run ~/myapp.jar # 启动应用
./bin/flink run ~/myapp.jar arg1 arg2

./bin/flink run -d ~/myapp.jar arg1 arg2 # 分离式提交任务

./bin/flink run -d -p 16 ~/myapp.jar arg1 arg2 # 指定并行度为16

./bin/flink run -d -p 16 -c my.app.Main ~/myapp.jar arg1 arg2 # 指定入口类

./bin/flink run -m myclusterhost:9876 -d -p 16 -c my.app.Main ~/myapp.jar arg1 arg2 # 将应用提交到指定的进程


# 列出任务
./bin/flink list -r 

# 生成保存点
./bin/flink savepoint <jobid> [savepointpath]
# 清除保存点
./bin/flink savepoint -d [savepointpath]

# 取消任务
./bin/flink cancel <jobid>
./bin/flink cancel -s [savepointpath] <jobid> # 同时生成保存点

./bin/flink run -s <savepointpath> [options] jobjar [arguments] # 从保存点启动

./bin/flink modify <jobid> -p <newparallelism> # 扩缩容
```

## 控制任务调度

```scala
// 禁用任务链
env.disableOperatorChaining()
```

### 定义处理槽共享组

Flink提供了处理槽共享组机制，允许用户将任务分配到处理槽中。

具体而言，每个算子都会属于一个处理槽共享组，所属同一处理槽共享组的算子，其任务都会由相同的处理槽处理。每个处理槽只能处理同一共享组内每个算子至多一个任务，因此一个处理槽共享组所需的处理槽数等于它内部算子的最大并行度。

属于不同处理槽共享组的算子，其任务会交由不同的处理槽执行。

默认情况，所有算子都属于“default”处理槽共享组。如果一个算子所有输入都属于同一处理槽共享组，那么该算子也会继承这个组；如果输入算子属于不同的处理槽共享组，那么该算子则会被加入到default组。

```scala
env
	.createInput()
	.slotSharingGroup("green")
	.setParallelism(4)
```

### 配置检查点

```scala
env.enableCheckpointing(10*1000L)
env.getCheckpointConfig.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE) // 至少一次
env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000) // 有效检查点生成间隔为30s
env.getCheckpointConfig.setMaxConcurrentCheckpoint(3) // 允许同时并发生成3个检查点。（检查点生成耗时长，但不会消耗太多资源）
env.getCheckpointConfig.setCheckpointTimeout
env.getCheckpointConfig.setCheckpointingErrors(false) // 不要因为检查点生成失败导致作业失败
env.getConfig.setUseSnapshotCompression(true) // 开启检查点压缩。rocksdb的增量检查点不支持

// 应用停止后保留检查点
env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
```

### 状态后端

```scala
val stateBackend = new RocksDBStateBackend("file:///tmp/ckp", true)
// . 状态后端配置
env.setStateBackend(stateBackend)
```

### 配置故障恢复

```scala
/*
fixed-delay: 重启策略会以配置的固定间隔重试固定的次数
failure-rate: 未超过故障率的前提下重启应用
no-restart
*/
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(30, TimeUnit.SECONDS)))
```

#### 本地恢复

Flink优先从本地恢复故障业务。如果失败，在从远处拉取远程检查点数据重启应用。

```yaml
state.backend.local-recovery: true
taskmanager.state.local.root-dirs: 本地状态副本位置
```

