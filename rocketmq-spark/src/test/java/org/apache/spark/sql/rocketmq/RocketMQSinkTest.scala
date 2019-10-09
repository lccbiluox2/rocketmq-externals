package org.apache.spark.sql.rocketmq

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.junit.Test

private[rocketmq] case class Person(id: String, name: String, age: String)

/**
  * @note
  * @author lcc
  * @version 1.0
  * @since create on 2019-10-09 14:48.
  **/
class RocketMQSinkTest  extends Logging {


  /**
    * 背景：要写一个Spark Structured Streaming 写入到 RocketMQ的实现
    *      该代码是从 https://github.com/lccbiluox2/rocketmq-externals.git 下载的代码
    * 测试点：测试Spark 写入到rocketMQ
    * 1,1,1
    */
  @Test
  def telnetToRocketMqTest220(): Unit ={

    val host = "localhost"
    val port = "9997"

    logInfo(String.format("监听主机：%s,端口：%s", host, port))

    val spark = SparkSession
      .builder
      .appName("testRocketMQ")
      .master("local[4]")
      .getOrCreate()

    val map = spark.conf.getAll.mkString("\n")
    logInfo(String.format("系统参数如下(jar生效测试,名称没有):\n %s ", map))


    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    lines.printSchema()

    val lineData =  lines.selectExpr("CAST(value AS STRING)").as(Encoders.STRING)
    import spark.implicits._

    val df = lineData.map(x => {
      val values = x.split(",")
      val id = values(0)
      val name = values(1)
      val age = values(2)
      new Person(id, name, age)
    })

    df.printSchema()
    df.createOrReplaceTempView("person")

    val lastData = spark.sql("select * from person");
    var valueDF = lastData.selectExpr("to_json(struct(*)) AS body")

    var streamReader = valueDF.writeStream
      .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation","/data/spark/checkpoint")
      .option("nameServer", "localhost:9876")
      .option("topic", "TopicTest")
      .trigger(Trigger.ProcessingTime(10000L, TimeUnit.MILLISECONDS))
      .queryName("sinkTest")

    val query = streamReader.start()
    query.awaitTermination()

  }

}
