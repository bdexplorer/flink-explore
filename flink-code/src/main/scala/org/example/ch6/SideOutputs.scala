package org.example.ch6

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object SideOutputs {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val readings: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val moinitorReadings: DataStream[SensorReading] = readings
			.process(new FreezingMonitor)

		moinitorReadings
			.getSideOutput(new OutputTag[String]("freezing-alarms"))
			.print()
		env.execute()
	}
}

class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
	lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")

	override def processElement(sn: SensorReading,
								ctx: ProcessFunction[SensorReading, SensorReading]#Context,
								out: Collector[SensorReading]): Unit = {
		if (sn.temperature < 32.0) {
			ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${sn.id}")
		}

		out.collect(sn)
	}
}