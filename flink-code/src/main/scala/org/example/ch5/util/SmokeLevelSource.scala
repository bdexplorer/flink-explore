package org.example.ch5.util

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.example.ch5.util.SmokeLevel.SmokeLevel

import scala.util.Random

class SmokeLevelSource extends RichParallelSourceFunction[SmokeLevel]{
	var running: Boolean = true
	override def run(sourceContext: SourceFunction.SourceContext[SmokeLevel]): Unit = {
		val rand = new Random()
		while (running) {
			if(rand.nextGaussian() > 0.8) {
				sourceContext.collect(SmokeLevel.High)
			} else  {
				sourceContext.collect(SmokeLevel.Low)
			}
			TimeUnit.MILLISECONDS.sleep(1000)
		}
	}

	override def cancel(): Unit = {
		running = false
	}
}
