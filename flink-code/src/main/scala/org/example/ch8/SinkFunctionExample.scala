package org.example.ch8

import java.io.PrintStream
import java.net.{InetAddress, Socket}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.example.bean.SensorReading

object SinkFunctionExample {

}

class SimpleSocketSink(val host: String, val port: Int) extends RichSinkFunction[SensorReading] {
	var socket: Socket = _
	var writer: PrintStream = _

	override def open(parameters: Configuration): Unit = {
		socket = new Socket(InetAddress.getByName(host), port)
		writer = new PrintStream(socket.getOutputStream)
	}

	override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
		writer.println(value.toString)
		writer.flush()
	}

	override def close(): Unit = {
		writer.close()
		socket.close()
	}
}
