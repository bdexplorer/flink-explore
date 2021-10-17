package org.example.ch8

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

object SourceFunctionExample {

}

class CountSource extends SourceFunction[Long] {
	var isRunning: Boolean = true

	override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
		var cnt: Long = -1;
		while (isRunning && cnt < Long.MaxValue) {
			cnt += 1
			ctx.collect(cnt)
		}
	}

	override def cancel(): Unit = {
		isRunning = false
	}
}

class ReplayableCountSource extends SourceFunction[Long] with CheckpointedFunction {
	var isRunning: Boolean = true
	var cnt: Long = _
	var offsetState: ListState[Long] = _

	override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
		while (isRunning && cnt < Long.MaxValue) {
			// 获取锁对象
			ctx.getCheckpointLock.synchronized {
				cnt += 1
				ctx.collect(cnt)

				// ctx.collectWithTimestamp(cnt, ts) 发出记录和与之关联的时间戳
				// ctx.emitWatermark() 发出水位线
				// ctx.markAsTemporarilyIdle()将数据源函数标记为空闲，在数据源空闲状态下，
				// Flink的水位线传播机制会忽略掉所有空闲的数据流分区。
			}
		}
	}

	override def cancel(): Unit = {
		isRunning = false
	}

	override def snapshotState(context: FunctionSnapshotContext): Unit = {
		offsetState.clear()
		offsetState.add(cnt)
	}

	override def initializeState(context: FunctionInitializationContext): Unit = {
		val desc = new ListStateDescriptor[Long]("offset", classOf[Long])
		offsetState = context.getOperatorStateStore.getListState(desc)

		val it = offsetState.get()
		cnt = if (null == it || !it.iterator().hasNext) {
			-1L
		} else {
			it.iterator().next()
		}
	}
}