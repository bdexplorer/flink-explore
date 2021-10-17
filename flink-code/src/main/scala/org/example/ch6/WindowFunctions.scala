package org.example.ch6

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object WindowFunctions {
	def threshold = 25.0

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sendData: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val minTempPerWindow: DataStream[(String, Double)] = sendData
			.map(s => (s.id, s.temperature))
			.keyBy(_._1)
			.timeWindow(Time.seconds(10))
			.reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

		val minTempPerWindow2: DataStream[(String, Double)] = sendData
			.map(s => (s.id, s.temperature))
			.keyBy(_._1)
			.timeWindow(Time.seconds(10))
			.reduce(new MinTempFunction)

		val avgTempPerWindow: DataStream[(String, Double)] = sendData
			.map(r => (r.id, r.temperature))
			.keyBy(_._1)
			.timeWindow(Time.seconds(15))
			.aggregate(new AvgTempFunction)

		val minMaxTempPerWindow: DataStream[MinMaxTemp] = sendData
			.keyBy(_.id)
			.timeWindow(Time.seconds(5))
			.process(new HighAndLowTempProcessFunction)

		sendData
			.map(r => (r.id, r.temperature, r.temperature))
			.keyBy(_._1)
			.timeWindow(Time.seconds(5))
			.reduce((r1: (String, Double, Double), r2: (String, Double, Double)) => {
				(r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
			}, new AssignWindowEndProcessFunction)
	}
}

class MinTempFunction extends ReduceFunction[(String, Double)] {
	override def reduce(r1: (String, Double), r2: (String, Double)): (String, Double) = {
		(r1._1, r1._2.min(r2._2))
	}
}

class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {
	override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

	override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
		(in._1, in._2 + acc._2, 1 + acc._3)
	}

	override def getResult(acc: (String, Double, Int)): (String, Double) = (acc._1, acc._2 / acc._3)

	override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = {
		(a._1, a._2 + b._2, a._3 + b._3)
	}
}

case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
	override def process(key: String,
						 context: Context,
						 elements: Iterable[SensorReading],
						 out: Collector[MinMaxTemp]): Unit = {
		val temps = elements.map(_.temperature)
		val windowEnd = context.window.getEnd
		out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
	}
}

class AssignWindowEndProcessFunction extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
	override def process(key: String,
						 context: Context,
						 minMaxIt: Iterable[(String, Double, Double)],
						 out: Collector[MinMaxTemp]): Unit = {
		val minMax = minMaxIt.head
		val windowEnd = context.window.getEnd

		out.collect(MinMaxTemp(key, minMax._2, minMax._3, windowEnd))
	}
}