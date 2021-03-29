package edu.sogang.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.IOException
import java.util.Properties
import scala.annotation.tailrec
import scala.sys.exit


object RunBench {

  def main(args: Array[String]): Unit = {

    val usage = """
    Usage:
      ./bin/spark-submit --class edu.sogang.benchmark.RunBench ASSEMBLED_JAR_PATH \
      --query-name QUERY_NAME --config-filename FILE_NAME
    """

    val argList = args.toList
    type ArgMap = Map[Symbol, Any]

    @tailrec
    def parseArgument(map : ArgMap, list: List[String]) : ArgMap = {
      list match {
        case Nil =>
          map
        case "--query-name" :: value :: tail =>
          parseArgument(map ++ Map('queryName -> value), tail)
        case "--config-filename" :: value :: tail =>
          parseArgument(map ++ Map('confFileName -> value), tail)
        case option :: tail =>
          println("Unknown option : " + option)
          println(usage)
          exit(1)
      }
    }

    val parsedArgs = parseArgument(Map(), argList)

    val query : QueryBase = parsedArgs('queryName) match {
      case "q1" =>
        new Query1()
      case "q2" =>
        new Query2()
      case other =>
        throw new Exception(s"$other is not supported")
    }

    val conf = new SparkConf()

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")

    conf.registerKryoClasses(
      Array(
        Class.forName("org.apache.spark.sql.types.StructType"),
        Class.forName("org.apache.spark.sql.types.StructField"),
        Class.forName("[Lorg.apache.spark.sql.catalyst.InternalRow;"),
        Class.forName("org.apache.spark.sql.kafka010.KafkaDataWriterCommitMessage$"),
        Class.forName("org.apache.spark.sql.execution.streaming.sources.PackedRowCommitMessage"),
        Class.forName("org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTaskResult"),
//        Class.forName("com.datastax.spark.connector.datasource.CassandraCommitMessage"),
      )
    )

    val ss = SparkSession.builder
//      .master("local[*]")
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel("WARN")

    val prop = new Properties()
    val inputSteam = this.getClass.getClassLoader.getResourceAsStream(parsedArgs('confFileName).toString)

    try {
      prop.load(inputSteam)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      inputSteam.close()
    }

    // kafka config
    val sourceTopic = prop.get("sourceTopic").asInstanceOf[String]
    val sinkTopic = prop.get("sinkTopic").asInstanceOf[String]
    val brokers = prop.get("brokers").asInstanceOf[String]
    val sinkCompressionType = prop.get("sinkCompressionType").asInstanceOf[String]
    val startingOffsets = prop.get("startingOffsets").asInstanceOf[String]
    val maxOffsetsPerTrigger = prop.get("maxOffsetsPerTrigger").asInstanceOf[String]
    val autoCommitOpt = prop.get("autoCommitOpt").asInstanceOf[String]

    // spark config
    val checkpointDir = prop.get("checkpointDir").asInstanceOf[String]

    // cassandra config
//    val cassandraKeyspace = prop.get("keyspace").asInstanceOf[String]
//    val cassandraCluster = prop.get("cluster").asInstanceOf[String]
//    val cassandraTable = prop.get("table").asInstanceOf[String]

    val df = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", sourceTopic)
      .option("enable.auto.commit", autoCommitOpt)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    val resultDf = query.runQuery(df)

    // console
//    resultDf.writeStream
//      .format("console")
//      .queryName("cluster-monitoring-benchmark-" + parsedArgs('queryName))
//      .option("checkpointLocation", checkpointDir)
//      .option("truncate", "false")
//      .start()
//      .awaitTermination()

    // kafka
    resultDf
      .writeStream
      .format("kafka")
      .queryName("cluster-monitoring-benchmark-" + parsedArgs('queryName))
      .outputMode("append")
      .option("checkpointLocation", checkpointDir)
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", sinkTopic)
      .option("kafka.compression.type", sinkCompressionType)
      .start()
      .awaitTermination()

    // cassandra
//      resultDf
//        .writeStream
//        .queryName("cluster-monitoring-benchmark-" + parsedArgs('queryName))
//        .cassandraFormat(cassandraTable, cassandraKeyspace, cassandraCluster)
//        .option("checkpointLocation", "/tmp/checkpoint-12314")
//        .start()
//        .awaitTermination()

  }
}
