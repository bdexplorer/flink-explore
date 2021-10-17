package org.example.ch8

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.example.bean.SensorReading

object IdempotentSinkFunctionExample {

}

// 幂等性数据汇连接器
// 1. 结果数据中用于幂等更新的键是确定的。
//    对于每分钟为每个传感器计算平均温度而言，其确定的键可以由传感器ID和每分钟的时间戳组成。
// 2. 外部系统支持按照键更新。例如关系型数据库或键值存储。
class DerbyUpsertSink extends RichSinkFunction[SensorReading] {

	var conn: Connection = _
	var insertStmt: PreparedStatement = _
	var updateStmt: PreparedStatement = _

	override def open(parameters: Configuration): Unit = {
		val props = new Properties()
		conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", props)
		insertStmt = conn.prepareStatement(
			"INSERT INTO Temperatures (sensor, temp) VALUES (?, ?)")
		updateStmt = conn.prepareStatement(
			"UPDATE Temperatures SET temp = ? WHERE sensor = ?")
	}

	override def invoke(r: SensorReading, context: Context[_]): Unit = {
		updateStmt.setDouble(1, r.temperature)
		updateStmt.setString(2, r.id)
		updateStmt.execute()
		if (updateStmt.getUpdateCount == 0) {
			insertStmt.setString(1, r.id)
			insertStmt.setDouble(2, r.temperature)
			insertStmt.execute()
		}
	}

	override def close(): Unit = {
		insertStmt.close()
		updateStmt.close()
		conn.close()
	}
}

