package org.example.ch5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object KeyedTransformations {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val reading: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val keyed: KeyedStream[SensorReading, String] = reading
			.keyBy(_.id)
		// == reading.keyBy(0) @deprecated
		// == reading.keyBy("id") @deprecated
		val sumed: DataStream[SensorReading] = keyed.sum(2)
		//		sumed.print()
		// minBy min maxBy max
		// min会返回对应字段的最小值，其他字段不能保证。
		// minBy会返回对应元素的所有值

		val maxTempPerSensor: DataStream[SensorReading] = keyed.reduce((r1, r2) => {
			if (r1.temperature > r2.temperature)
				r1
			else
				r2
		})
		maxTempPerSensor.print()

		env.execute()
	}
}
