package org.example.ch7

import java.util.Collections
import java.{lang, util}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

import scala.collection.JavaConverters._

object OperatorListStateFunction {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sensorData: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val highTempCounts: DataStream[(Int, Long)] = sensorData.flatMap(new HighTempCounterOpState(12.0))
		highTempCounts.print()
		env.execute()
	}
}

class HighTempCounterOpState(val threshold: Double)
	extends RichFlatMapFunction[SensorReading, (Int, Long)]
		with ListCheckpointed[java.lang.Long] {

	// 子任务索引
	private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask

	// 本地计数器变量
	private var highTempCnt = 0L

	override def flatMap(in: SensorReading, out: Collector[(Int, Long)]): Unit = {
		if (in.temperature > threshold) {
			// 如果超过阈值，计数器加1
			highTempCnt += 1
			out.collect((subtaskIdx, highTempCnt))
		}
	}
	// 为了在扩缩容操作，可以将状态拆分
	override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Long] = {
		// 将一个包含单条数据值的列表作为状态快照
		Collections.singletonList(highTempCnt)
	}

	override def restoreState(state: util.List[lang.Long]): Unit = {
		highTempCnt = 0
		// 将状态恢复为列表中的全部long值之和
		state.asScala.foreach(cnt => highTempCnt + cnt)
	}
}