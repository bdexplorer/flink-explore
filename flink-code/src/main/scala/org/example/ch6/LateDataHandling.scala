package org.example.ch6

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

import scala.util.Random

object LateDataHandling {
	val lateReadingsOutput = new OutputTag[SensorReading]("late-readings")

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val outOfOrderReadings: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.map(new TimestampShuffler(7 * 1000))
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		filterLateReadings(outOfOrderReadings)
		env.execute()

	}

	def filterLateReadings(readings: DataStream[SensorReading]): Unit = {
		val filteredReadings: DataStream[SensorReading] = readings
			.process(new LateReadingsFilter)
		val lateReadings: DataStream[SensorReading] = filteredReadings
			.getSideOutput(lateReadingsOutput)
		filteredReadings.print()
		lateReadings
			.map(r => "*** late reading ***" + r.id)
			.print()
	}

	def sideOutputLateEventsWindow(readings: DataStream[SensorReading]): Unit = {
		val countPer10Secs: DataStream[(String, Long, Int)] = readings
			.keyBy(_.id)
			.timeWindow(Time.seconds(10))
			.sideOutputLateData(lateReadingsOutput)
			.process(new ProcessWindowFunction[SensorReading, (String, Long, Int), String, TimeWindow] {
				override def process(key: String,
									 context: Context,
									 elements: Iterable[SensorReading],
									 out: Collector[(String, Long, Int)]): Unit = {
					val cnt = elements.count(_ => true)
					out.collect((key, context.window.getEnd, cnt))
				}
			})

		countPer10Secs
			.getSideOutput(lateReadingsOutput)
			.map(r => "*** late reading ***" + r.id)
			.print()
		countPer10Secs.print()
	}

	def updateForLateEventsWindow(readings: DataStream[SensorReading]): Unit = {
		val countPer10s: DataStream[(String, Long, Int, String)] = readings
			.keyBy(_.id)
			.timeWindow(Time.seconds(10))
			.allowedLateness(Time.seconds(5))
			.process(new UpdatingWindowCountFunction)

		countPer10s.print()
	}
}

class LateReadingsFilter extends ProcessFunction[SensorReading, SensorReading] {
	override def processElement(sr: SensorReading,
								ctx: ProcessFunction[SensorReading, SensorReading]#Context,
								out: Collector[SensorReading]): Unit = {
		if (sr.timestamp < ctx.timerService().currentWatermark()) {
			ctx.output(LateDataHandling.lateReadingsOutput, sr)
		} else {
			out.collect(sr)
		}
	}
}

class UpdatingWindowCountFunction extends ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow] {
	override def process(key: String,
						 context: Context,
						 elements: Iterable[SensorReading],
						 out: Collector[(String, Long, Int, String)]): Unit = {
		val cnt = elements.count(_ => true)
		val isUpdate = context
			.windowState
			.getState(new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean]))

		if (!isUpdate.value()) {
			out.collect((key, context.window.getEnd, cnt, "first"))
			isUpdate.update(true)
		} else {
			out.collect((key, context.window.getEnd, cnt, "update"))
		}
	}
}

class TimestampShuffler(maxRandomOffset: Int) extends MapFunction[SensorReading, SensorReading] {
	lazy val rand: Random = new Random()

	override def map(sr: SensorReading): SensorReading = {
		val shuffleTs = sr.timestamp + rand.nextInt(maxRandomOffset)
		SensorReading(sr.id, shuffleTs, sr.temperature)
	}
}
