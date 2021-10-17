package org.example.ch5

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object BasicTransformations {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val reading: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		// 过滤所有温度低于25华氏的传感器测量值
		val filteredSensors: DataStream[SensorReading] =
			reading.filter(_.temperature >= 25)

		// map: 提取sensorId
		val sensorIds: DataStream[String] = filteredSensors.map(_.id)

		//val splitIds: DataStream[String] = sensorIds.flatMap(_ split "_")

		val splitIds: DataStream[String] = sensorIds.flatMap(new SplitIdFlatMap)
		// filteredSensors.print()
		// sensorIds.print()
		splitIds.print()

		env.execute()
	}

	class TemperatureFilter(threshold: Long) extends FilterFunction[SensorReading] {
		override def filter(r: SensorReading): Boolean = r.temperature >= threshold
	}

	class ProjectionMap extends MapFunction[SensorReading, String] {
		override def map(r: SensorReading): String = r.id
	}

	class SplitIdFlatMap extends FlatMapFunction[String, String] {
		override def flatMap(id: String, out: Collector[String]): Unit = {
			val ids: Array[String] = id.split("_")
			ids.foreach(id => out.collect(id))
		}
	}

}
