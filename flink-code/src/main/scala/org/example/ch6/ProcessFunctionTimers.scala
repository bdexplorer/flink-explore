package org.example.ch6

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.bean.SensorReading
import org.example.source.SensorSource

object ProcessFunctionTimers {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

		val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

		val warnings = readings
			.keyBy(_.id)
			.process(new TempIncreaseAlertFunction)
		warnings.print()
		env.execute()
	}
}

// 如果传感器的温度在1s内持续上升，则发出告警
class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
	// 存储最近一次传感器温度读数
	lazy val lastTemp: ValueState[Double] = getRuntimeContext
		.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

	// 存储当前活动计时器的时间戳
	lazy val currentTimer: ValueState[Long] = getRuntimeContext
		.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

	override def processElement(sensorReading: SensorReading,
								ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
								out: Collector[String]): Unit = {

		val preTemp = lastTemp.value()
		lastTemp.update(sensorReading.temperature)

		val curTimerTimestamp = currentTimer.value()
		if (preTemp == 0.0) {

		}
		else if (sensorReading.temperature < preTemp) {
			ctx.timerService().deleteEventTimeTimer(curTimerTimestamp)
			currentTimer.clear()
		}
		// 检测温度在1s内是否持续增加
		else if (sensorReading.temperature > preTemp && curTimerTimestamp == 0) {
			val timeTs = ctx.timerService().currentProcessingTime() + 1000
			ctx.timerService().registerProcessingTimeTimer(timeTs)
			currentTimer.update(timeTs)
		}
	}

	override def onTimer(timestamp: Long,
						 ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
						 out: Collector[String]): Unit = {
		out.collect("Temperature of sensor " + ctx.getCurrentKey + " monotonically increased for 1 second")
		currentTimer.clear()
	}
}