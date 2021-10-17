package org.example.source

import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.example.bean.SensorReading

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {
	@volatile var running: Boolean = true

	override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
		val rand = new Random()
		val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
		var curFTemp = (1 to 10).map(i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20)))
		while (running) {
			val curTime = System.currentTimeMillis()
			curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
			curFTemp.foreach(t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))
			TimeUnit.MILLISECONDS.sleep(100)
		}
	}

	override def cancel(): Unit = {
		running = false
	}
}
