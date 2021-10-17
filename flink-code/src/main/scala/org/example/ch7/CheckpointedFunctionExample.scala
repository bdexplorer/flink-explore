package org.example.ch7

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

import scala.collection.JavaConverters._

object CheckpointedFunctionExample {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sensorData: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val highTempCnts: DataStream[(String, Double, Double)] = sensorData
			.keyBy(_.id)
			.flatMap(new HighTempCounter(10.0))
		highTempCnts.print()
		env.execute()
	}
}

class HighTempCounter(val threshold: Double)
	extends FlatMapFunction[SensorReading, (String, Double, Double)]
		with CheckpointedFunction {
	// 在本地用于存储算子实例高温数目的变量
	var opHighTempCnt: Long = 0

	var keyedCntState: ValueState[Long] = _
	var opCntState: ListState[Long] = _

	override def flatMap(sr: SensorReading,
						 out: Collector[(String, Double, Double)]): Unit = {
		if (sr.temperature > threshold) {
			opHighTempCnt += 1
			val keyHighTempCnt = keyedCntState.value() + 1
			keyedCntState.update(keyHighTempCnt)
			out.collect((sr.id, keyHighTempCnt, opHighTempCnt))
		}
	}

	override def snapshotState(context: FunctionSnapshotContext): Unit = {
		// 更新本地的状态更新算子状态
		opCntState.clear()
		opCntState.add(opHighTempCnt)
	}

	override def initializeState(initContext: FunctionInitializationContext): Unit = {
		// 初始化键值分区状态
		val keyCntDescriptor = new ValueStateDescriptor[Long]("keyedCnt", classOf[Long])
		keyedCntState = initContext.getKeyedStateStore.getState(keyCntDescriptor)

		val opCntDescriptor = new ListStateDescriptor[Long]("opCnt", classOf[Long])
		opCntState = initContext.getOperatorStateStore.getListState(opCntDescriptor)

		opHighTempCnt = opCntState.get().asScala.sum
	}
}