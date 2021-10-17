package org.example.ch6

import java.{lang, util}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object CustomWindows {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sensorData: DataStream[SensorReading] = env
    		.addSource(new SensorSource)
    		.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val countPerThirtySecs: DataStream[(String, Long, Long, Int)] = sensorData
			.keyBy(_.id)
			.window(new ThirdtySecondsWindows)
			.trigger(new OneSecondIntervalTrigger)
			.process(new CountFunction)
		countPerThirtySecs.print()
		env.execute()
	}
}

class ThirdtySecondsWindows extends WindowAssigner[Object, TimeWindow] {
	val windowSize: Long = 30 * 1000L

	override def assignWindows(obj: Object,
							   timestamp: Long,
							   context: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = {
		// 30s取余，最终是30s整数倍。
		val startTime = timestamp - (timestamp % windowSize)
		val endTime = startTime + windowSize
		util.Collections.singletonList(new TimeWindow(startTime, endTime))
	}

	override def getDefaultTrigger(env: environment.StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
		EventTimeTrigger.create()
	}

	override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
		new TimeWindow.Serializer
	}

	override def isEventTime: Boolean = true
}

class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
	override def onElement(element: SensorReading,
						   timestamp: Long,
						   window: TimeWindow,
						   ctx: Trigger.TriggerContext): TriggerResult = {
		val firstSeen: ValueState[Boolean] = ctx.
			getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
		if (!firstSeen.value()) {
			// 取整到秒计算下一次触发时间（整秒）
			val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
			ctx.registerEventTimeTimer(window.getEnd)
			firstSeen.update(true)
		}
		TriggerResult.CONTINUE
	}

	override def onProcessingTime(time: Long,
								  window: TimeWindow,
								  ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

	override def onEventTime(time: Long,
							 window: TimeWindow,
							 ctx: Trigger.TriggerContext): TriggerResult = {
		if (time == window.getEnd) {
			// 进行最终计算并发出结果
			TriggerResult.FIRE_AND_PURGE
		} else {
			// 注册下一个用于提前触发计算的计时器
			val t: Long = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
			if (t < window.getEnd) {
				ctx.registerEventTimeTimer(t)
			}
			TriggerResult.FIRE
		}
	}

	override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
		val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(
			new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
		firstSeen.clear()
	}
}

class CountFunction extends ProcessWindowFunction[SensorReading, (String, Long, Long, Int), String, TimeWindow] {
	override def process(key: String,
						 context: Context,
						 elements: Iterable[SensorReading],
						 out: Collector[(String, Long, Long, Int)]): Unit = {
		val cnt = elements.count(_ => true)
		val evalTime = context.currentWatermark
		out.collect((key, context.window.getEnd, evalTime, cnt))
	}
}