package org.example.ch5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.ch5.util.SmokeLevel.SmokeLevel
import org.example.ch5.util.{Alert, SmokeLevel, SmokeLevelSource}
import org.example.source.SensorSource

object MultiStreamTransformation {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val tempReading: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)


		val smokeReading: DataStream[SmokeLevel] = env
			.addSource(new SmokeLevelSource)
			.setParallelism(1)

		val keyed: KeyedStream[SensorReading, String] = tempReading.keyBy(_.id)

		val alerts: DataStream[Alert] = keyed
			.connect(smokeReading.broadcast)
			.flatMap(new RaiseAlertFlatMap)
		alerts.print()
		env.execute()
	}

	class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading, SmokeLevel, Alert] {
		var smokeLevel = SmokeLevel.Low

		override def flatMap1(in1: SensorReading, collector: Collector[Alert]): Unit = {
			if (smokeLevel.equals(SmokeLevel.High) && in1.temperature > 100) {
				collector.collect(Alert("Risk ofo fire!", in1.timestamp))
			}
		}

		override def flatMap2(in2: SmokeLevel, collector: Collector[Alert]): Unit = {
			smokeLevel = in2
		}
	}

}
