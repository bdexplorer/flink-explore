package org.example.ch5

import org.apache.flink.streaming.api.scala._

object RollingSum {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
			(1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

		val resultStream: DataStream[(Int, Int, Int)] = inputStream
			.keyBy(0) // key on first field of the tuple
			.sum(1) // sum the second field of the tuple

		val minStream: DataStream[(Int, Int, Int)] = inputStream
			.keyBy(0)
			.min(1)

		val minByStream: DataStream[(Int, Int, Int)] = inputStream
			.keyBy(0)
			.minBy(1)

		// resultStream.print()
		minByStream.print()

		// execute application
		env.execute("Rolling Sum Example")
	}
}
