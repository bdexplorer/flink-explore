package org.example.ch7

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object KeyedStatedFunction {
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
			.flatMap(new TemperatureAlertFunction(1.7))
		alerts.print()
		env.execute()
	}

	// 建议统一使用RichFlatMapFunction
	def flatMapWithState(keySensorData: KeyedStream[SensorReading, String]): Unit = {
		// flink api为只有单个ValueState的map和flatMap等函数提供了更为简洁的方法。 比如flatMapWithState
		val alerts: DataStream[(String, Double, Double)] = keySensorData.flatMapWithState[(String, Double, Double), Double] {
			// 之前温度还没有定义，只需要更新一个温度值
			case (in: SensorReading, None) => (List.empty, Some(in.temperature))
			case (r: SensorReading, lastTemp: Some[Double]) =>
				val tempDiff = (r.temperature - lastTemp.get).abs
				if (tempDiff > 1.7) {
					(List((r.id, r.temperature, tempDiff)), Some(r.temperature))
				} else {
					(List.empty, Some(r.temperature))
				}
		}
		alerts.print()
	}
}

class TemperatureAlertFunction(val threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
	// 状态引用对象
	private var lastTempState: ValueState[Double] = _


	override def open(parameters: Configuration): Unit = {
		// 创建状态描述符
		val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
		// 获得状态引用
		lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
	}

	override def flatMap(reading: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
		val lastTemp = lastTempState.value()
		val tempDiff = (reading.timestamp - lastTemp).abs

		if (tempDiff > threshold) {
			out.collect((reading.id, reading.temperature, tempDiff))
		}
		this.lastTempState.update(reading.temperature)
	}
}
