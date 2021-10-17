package org.example.ch7

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object BroadcastStateFunction {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sensorData: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val thresholds: DataStream[ThresholdUpdate] = env
			.fromElements(
				ThresholdUpdate("sensor_1", 5.0d),
				ThresholdUpdate("sensor_2", 0.9d),
				ThresholdUpdate("sensor_3", 0.5d),
				ThresholdUpdate("sensor_1", 1.2d),
				ThresholdUpdate("sensor_3", 0.0d)
			)

		val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)
		// 广播状态描述符
		val broadcastStateDescriptor =
			new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])
		val broadcastThesholds: BroadcastStream[ThresholdUpdate] = thresholds
			.broadcast(broadcastStateDescriptor)
		// 连接键值分区传感数据流和广播的规则
		val alerts: DataStream[(String, Double, Double)] = keyedSensorData
			.connect(broadcastThesholds)
			.process(new UpdatableTemperatureAlertFunction)
		alerts.print()
		env.execute()
	}

}

case class ThresholdUpdate(id: String, threshold: Double)

class UpdatableTemperatureAlertFunction
	extends KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)] {
	// 广播状态的描述符
	private lazy val thresholdStateDescriptor =
		new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

	// 键值分区状态引用对象
	private var lastTempState: ValueState[Double] = _

	override def open(parameters: Configuration): Unit = {
		val lastTempDescriptor =
			new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
		lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
	}

	override def processElement(reading: SensorReading,
								ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#ReadOnlyContext,
								out: Collector[(String, Double, Double)]): Unit = {
		// 获取只读的广播状态
		val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)
		// 判断阈值是否已经存在
		if (thresholds.contains(reading.id)) {
			val sensorThreshold: Double = thresholds.get(reading.id)
			val lastTemp = lastTempState.value()
			val tempDiff = (reading.temperature - lastTemp).abs
			if (tempDiff > sensorThreshold) {
				out.collect((reading.id, reading.temperature, tempDiff))
			}
		}
		this.lastTempState.update(reading.temperature)
	}

	override def processBroadcastElement(update: ThresholdUpdate,
										 ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#Context,
										 out: Collector[(String, Double, Double)]): Unit = {
		// 获取只读的广播状态
		val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)
		if (update.threshold != 0.0d) {
			// 指定配置新的阈值
			thresholds.put(update.id, update.threshold)
		} else {
			// 删除该传感器的阈值
			thresholds.remove(update.id)
		}
	}
}