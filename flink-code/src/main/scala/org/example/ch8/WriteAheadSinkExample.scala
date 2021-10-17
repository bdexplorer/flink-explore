package org.example.ch8

import java.nio.file.{Files, Paths}
import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.operators.{CheckpointCommitter, GenericWriteAheadSink}

import scala.collection.JavaConverters._

object WriteAheadSinkExample {

}

// GenericWriteAheadSink 模板会收集每个检查点周期内所有需要写出的记录，并将它们存储到数据汇任务的算子状态中。
// 	该状态会被写入检查点，并在故障时恢复。
// 	当一个任务接收到检查点完成通知时，会将此次检查点周期内的所有记录写入外部系统。
// 基于WAL的数据汇在某些情况下，可能会将记录写出多次，因此GenericWriteAheadSink只能做到至少一次
// 原理：
// 	1. 将经由各个检查点“分段”后的接收记录以追加形式写入WAL中。数据会每收到一个检查点分隔符，
// 		都会生成一个新的“记录章节”，并将接下来的所有记录追加写入该章节。
// 		WAL会以算子状态的形式存储和写入检查点。由于它在发生故障时可以恢复，所以不会导致数据丢失。
//  2. 当GenericWriteAheadSink接收到检查点完成通知时，会将WAL内所有对应该检查点的记录发出。
//  	当所有记录发出成功后，数据汇需要在内部提交对应的检查点。
//	内部检查点提交分为两步：
//		1、数据汇需要将检查点已提交的信息持久化
//		2、它会从WAL中删除相应的记录
//	依赖CheckpointCommiter的可插拔组件控制外部持久化系统存储和查找已提交的检查点信息。
// 实现：
// 构造参数：
// 	1、一个之前介绍过的CheckpointCommitter
// 	2、一个用于序列化输入记录的TypeSerializer
// 	3、一个传递给CheckpointCommitter，用于应用重启后标识提交信息的任务ID
// sendValues
class StdOutWriteAheadSink extends GenericWriteAheadSink[(String, Double)](
	new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
	createTypeInformation[(String, Double)].createSerializer(new ExecutionConfig),
	UUID.randomUUID.toString) {

	override def sendValues(readings: java.lang.Iterable[(String, Double)],
							checkpointId: Long,
							timestamp: Long): Boolean = {
		readings.asScala.foreach(println)
		true
	}
}

class FileCheckpointCommitter(val basePath: String) extends CheckpointCommitter {

	private var tempPath: String = _

	override def commitCheckpoint(subtaskIdx: Int, checkpointID: Long): Unit = {
		val commitPath = Paths.get(tempPath + "/" + subtaskIdx)
		val hexID = "0x" + StringUtils.leftPad(checkpointID.toHexString, 16, "0")
		Files.write(commitPath, hexID.getBytes)
	}

	override def isCheckpointCommitted(subtaskIdx: Int, checkpointID: Long): Boolean = {
		val commitPath = Paths.get(tempPath + "/" + subtaskIdx)

		if (!Files.exists(commitPath)) {
			false
		} else {
			val hexID = Files.readAllLines(commitPath).get(0)
			val checkpointed = java.lang.Long.decode(hexID)
			checkpointID <= checkpointed
		}
	}

	override def createResource(): Unit = {
		this.tempPath = basePath + "/" + this.jobId
		Files.createDirectory(Paths.get(tempPath))
	}

	override def open(): Unit = {
	}

	override def close(): Unit = {
	}
}