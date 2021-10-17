package org.example.assigner

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.example.bean.SensorReading

class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
	val bound: Long = 60 * 1000 // 1min
	var maxTs: Long = Long.MinValue // 记录观察到的最大时间戳

	override def getCurrentWatermark: Watermark = {
		new Watermark(maxTs - bound) // 生成具有1分钟容忍度的水位线
	}

	override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
		maxTs = maxTs.max(element.timestamp) // 更新最大时间戳
		element.timestamp
	}
}
