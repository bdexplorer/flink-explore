package org.example.ch1

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object AverageSensorReading {
	def main(args: Array[String]): Unit = {
		// 1. 设置流式执行环境
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		// 在应用中使用事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sensorData = env
			.addSource(new SensorSource) // 利用SensorSource SourceFunction获取传感器读书
			.assignTimestampsAndWatermarks(new SensorTimeAssigner) // 分配时间戳和水位线

		val avgTemp: DataStream[SensorReading] = sensorData
			.map(r => SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0))) // 转为摄氏温度
			.keyBy(_.id) // 根据id分区
			.timeWindow(Time.seconds(1)) // 定义1秒的滚动窗口
			.apply(new TemperatureAverager) // 自定义函数计算平均温度

		// 输出
		avgTemp.print()

		// 执行应用，Flink程序都是通过延迟计算的方式执行。也就是说，那些创建数据源和转换操作的API调用不会立即触发数据处理，而只会在执行环境中构建一个执行计划。只有调用execute()方法时，系统才会触发计算。
		env.execute("Compute average sensor temperature")
	}
}

class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {
	override def apply(key: String,
					   window: TimeWindow,
					   input: Iterable[SensorReading],
					   out: Collector[SensorReading]): Unit = {
		val (cnt, sum) = input.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
		val avgTemp = sum / cnt
		out.collect(SensorReading(key, window.getEnd, avgTemp))
	}
}