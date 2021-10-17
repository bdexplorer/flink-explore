package org.example.assigner

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.example.bean.SensorReading

// 直到输入流中的延迟使用BoundedOutOfOrdernessTimestampExtractor。
// 元素最多允许延迟5s
class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
	override def extractTimestamp(t: SensorReading): Long = t.timestamp
}
