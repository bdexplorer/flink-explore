package org.example.ch6

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.bean.SensorReading
import org.example.source.SensorSource

object CoProcessFunctionTimers {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

		val filterSwitchs: DataStream[(String, Long)] = env
			.fromCollection(Seq(("sensor_2", 10 * 1000L), ("sensor_7", 1 * 1000L))) // sensor2转发1min， sensor7转发1s

		val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

		val forwardedReading: DataStream[SensorReading] = readings
			.connect(filterSwitchs)
			.keyBy(_.id, _._1)
			.process(new ReadingFilter)

		forwardedReading.print()
		env.execute()
	}
}

class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
	lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext
		.getState(new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean]))

	lazy val disableTimer: ValueState[Long] = getRuntimeContext
		.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

	override def processElement1(sr: SensorReading,
								 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
								 out: Collector[SensorReading]): Unit = {
		if (forwardingEnabled.value()) {
			out.collect(sr)
		}
	}

	override def processElement2(switch: (String, Long),
								 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
								 out: Collector[SensorReading]): Unit = {
		forwardingEnabled.update(true)
		val timerTimeStamp = ctx.timerService().currentProcessingTime() + switch._2
		val curTimerTimestamp = disableTimer.value()
		if (timerTimeStamp > curTimerTimestamp) {
			ctx.timerService().deleteEventTimeTimer(curTimerTimestamp)
			ctx.timerService().registerProcessingTimeTimer(timerTimeStamp)
			disableTimer.update(timerTimeStamp)
		}
	}

	override def onTimer(timestamp: Long,
						 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
						 out: Collector[SensorReading]): Unit = {
		forwardingEnabled.clear()
		disableTimer.clear()
	}
}
