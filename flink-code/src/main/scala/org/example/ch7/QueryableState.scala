package org.example.ch7

import java.util.concurrent.CompletableFuture

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.example.assigner.SensorTimeAssigner
import org.example.bean.SensorReading
import org.example.source.SensorSource

object TrackMaximumTemperature {
	def main(args: Array[String]) {
		val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
		env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(1000L)

		val sensorData: DataStream[SensorReading] = env
			.addSource(new SensorSource)
			.assignTimestampsAndWatermarks(new SensorTimeAssigner)

		val tenSecsMaxTemps: DataStream[(String, Double)] = sensorData
			.map(r => (r.id, r.temperature))
			.keyBy(_._1)
			.timeWindow(Time.seconds(10))
			.max(1)

		// 支持利用数据汇将流中所有事件都存到可查询式状态中。
		tenSecsMaxTemps
			.keyBy(_._1)
			.asQueryableState("maxTemperature")

		env.execute("Track max temperature")
	}
}

object TemperatureDashboard {

	val proxyHost = "127.0.0.1"
	val proxyPort = 9069
	val jobId = "d2447b1a5e0d952c372064c886d2220a"
	val numSensors = 5
	val refreshInterval = 10000

	def main(args: Array[String]): Unit = {

		val client = new QueryableStateClient(proxyHost, proxyPort)
		val futures = new Array[CompletableFuture[ValueState[(String, Double)]]](numSensors)
		val results = new Array[Double](numSensors)

		val header = (for (i <- 0 until numSensors) yield "sensor_" + (i + 1)).mkString("\t| ")
		println(header)

		while (true) {
			for (i <- 0 until numSensors) {
				futures(i) = queryState("sensor_" + (i + 1), client)
			}
			for (i <- 0 until numSensors) {
				results(i) = futures(i).get().value()._2
			}
			val line = results.map(t => f"$t%1.3f").mkString("\t| ")
			println(line)

			Thread.sleep(refreshInterval)
		}

		client.shutdownAndWait()

	}

	def queryState(key: String, client: QueryableStateClient): CompletableFuture[ValueState[(String, Double)]] = {
		client.getKvState(
			JobID.fromHexString(jobId),
			"maxTemperature",
			key,
			Types.STRING,
			new ValueStateDescriptor[(String, Double)]("", Types.TUPLE[(String, Double)]))
	}

}
