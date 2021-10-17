package org.example.ch5

import org.apache.flink.streaming.api.scala._

object SplitTransformations {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
			(1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

		val splited: SplitStream[(Int, Int, Int)] = inputStream
			.split(s => if (s._1 == 1) Seq("small") else Seq("large"))
		val smallStream: DataStream[(Int, Int, Int)] = splited.select("small")
		smallStream.print()
		env.execute()
	}
}
