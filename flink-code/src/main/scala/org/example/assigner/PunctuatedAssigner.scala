package org.example.assigner

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.example.bean.SensorReading

class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {
	val bound: Long = 60 * 1000L

	override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
		if(lastElement.id == "Sensor_1") {
			// 如果读数来自Sensor_1则发出水位线
			new Watermark(extractedTimestamp - bound)
		} else {
			// 不发出水位线
			null
		}
	}

	override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
		element.timestamp
	}
}
