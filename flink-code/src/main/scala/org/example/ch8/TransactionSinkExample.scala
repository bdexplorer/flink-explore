package org.example.ch8

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
import org.apache.flink.streaming.api.scala._

object TransactionSinkExample {

}

// TwoPhaseCommitSinkFunction 充分利用了外部数据汇系统的事务功能。对于每个检查点，它都会开启一个新的事务，
// 	并以当前事务为上下文将所有后续记录写入数据汇系统。数据汇在接收到对应检查点的完成通知后才会提交事务。

// TwoPhaseCommitSinkFunction可以实现端对端的精确一次语义保证。
// TwoPhaseCommitSinkFunction工作原理和基于WAL的数据汇类似，但它不会在Flink的应用状态中收集记录，
// 而是会把它们写入外部数据汇系统某个开启的事务中。
// TwoPhaseCommitSinkFunction：数据汇任务在发出首个记录之前，会先在外部数据汇系统开启一个事务。

// 1. 外部数据汇系统支持事务。否则模拟外部系统的事务
// 2. 在检查点的间隔期内，事务需要保持开启状态并允许数据写入
// 3. 事务只有在接收到检查点完成通知后才可以提交。
// 4. 故障事务恢复
// 5. 事务提交是幂等的

// 写文件的事务性数据汇
class TransactionalFileSink(val targetPath: String, val tempPath: String)
	extends TwoPhaseCommitSinkFunction[(String, Double), String, Void](
		createTypeInformation[String].createSerializer(new ExecutionConfig),
		createTypeInformation[Void].createSerializer(new ExecutionConfig)) {

	var transactionWriter: BufferedWriter = _

	/**
	 * 为写入记录的事务创建一个临时文件
	 */
	override def beginTransaction(): String = {
		// 事务文件的路径，由当前时间和任务索引决定
		val timeNow = LocalDateTime.now(ZoneId.of("UTC"))
			.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
		val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
		val transactionFile = s"$timeNow-$taskIdx"

		// create transaction file and writer
		val tFilePath = Paths.get(s"$tempPath/$transactionFile")
		Files.createFile(tFilePath)
		this.transactionWriter = Files.newBufferedWriter(tFilePath)
		println(s"Creating Transaction File: $tFilePath")

		// name of transaction file is returned to later identify the transaction
		transactionFile
	}

	/**
	 * 将记录写入当前事务文件中
	 */
	override def invoke(transaction: String, value: (String, Double), context: Context[_]): Unit = {
		transactionWriter.write(value.toString)
		transactionWriter.write('\n')
	}

	/**
	 *  强制写出文件内容并关闭当前事务文件
	 */
	override def preCommit(transaction: String): Unit = {
		transactionWriter.flush()
		transactionWriter.close()
	}

	/**
	 * 通过将预提交的事务文件移动到目标目录来提交事务
	 */
	override def commit(transaction: String): Unit = {
		val tFilePath = Paths.get(s"$tempPath/$transaction")
		// check if the file exists to ensure that the commit is idempotent.
		if (Files.exists(tFilePath)) {
			val cFilePath = Paths.get(s"$targetPath/$transaction")
			Files.move(tFilePath, cFilePath)
		}
	}

	/**
	 * 通过删除事务文件来终止事务
	 */
	override def abort(transaction: String): Unit = {
		val tFilePath = Paths.get(s"$tempPath/$transaction")
		if (Files.exists(tFilePath)) {
			Files.delete(tFilePath)
		}
	}
}