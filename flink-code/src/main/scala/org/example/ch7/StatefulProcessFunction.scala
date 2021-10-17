package org.example.ch7

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object StatefulProcessFunction {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sensorData: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)
		val alerts: DataStream[(String, Double, Double)] = keyedSensorData
			.process(new SelfCleaningTemperatureAlertFunction(1.5))
		alerts.print()
		env.execute()
	}
}

// 可以清除一段时间没有使用的键值状态
class SelfCleaningTemperatureAlertFunction(val threshold: Double)
	extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

	private var lastTempState: ValueState[Double] = _
	private var lastTimerState: ValueState[Long] = _


	override def open(parameters: Configuration): Unit = {
		val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
		lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)

		val timestampDescriptor: ValueStateDescriptor[Long] =
			new ValueStateDescriptor[Long]("timestampState", classOf[Long])
		lastTimerState = getRuntimeContext.getState(timestampDescriptor)
	}

	override def processElement(reading: SensorReading,
								ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context,
								out: Collector[(String, Double, Double)]): Unit = {
		val newTimer = ctx.timestamp() + (3600 * 1000)
		val curTimer = lastTimerState.value()
		ctx.timerService().deleteEventTimeTimer(curTimer)
		ctx.timerService().registerEventTimeTimer(newTimer)
		lastTimerState.update(newTimer)

		val lastTemp = lastTempState.value()
		val tempDiff = (reading.temperature - lastTemp).abs
		if (tempDiff > threshold) {
			out.collect((reading.id, reading.temperature, tempDiff))
		}
		this.lastTempState.update(reading.temperature)
	}

	override def onTimer(timestamp: Long,
						 ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#OnTimerContext,
						 out: Collector[(String, Double, Double)]): Unit = {
		lastTempState.clear()
		lastTimerState.clear()
	}
}