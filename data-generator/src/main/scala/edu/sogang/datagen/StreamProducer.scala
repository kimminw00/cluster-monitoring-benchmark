package edu.sogang.datagen

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.{File, IOException}
import java.lang.Thread.sleep
import java.util.Properties

import scala.annotation.tailrec
import scala.io.BufferedSource
import scala.sys.exit


object StreamProducer {
  def main(args: Array[String]): Unit = {

    val usage = """
    Usage: java -jar ASSEMBLED_JAR_PATH --config-filename FILE_NAME
    """

    val argList = args.toList
    type ArgMap = Map[Symbol, Any]

    @tailrec
    def parseArgument(map : ArgMap, list: List[String]) : ArgMap = {
      list match {
        case Nil =>
          map
        case "--config-filename" :: value :: tail =>
          parseArgument(map ++ Map('confFileName -> value), tail)
        case option :: tail =>
          println("Unknown option : " + option)
          println(usage)
          exit(1)
      }
    }

    val parsedArgs = parseArgument(Map(), argList)

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

    // app config
    val msgInterval = prop.get("msgInterval").asInstanceOf[String].toInt
    val dataDir = prop.get("dataDir").asInstanceOf[String]

    // kafka config
    val topic = prop.get("topic").asInstanceOf[String]
    val bootstrapServers = prop.get("bootstrapServers").asInstanceOf[String]
    val keySerializer = prop.get("keySerializer").asInstanceOf[String]
    val valueSerializer = prop.get("valueSerializer").asInstanceOf[String]
    val compressionType = prop.get("compressionType").asInstanceOf[String]

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", keySerializer)
    props.put("value.serializer", valueSerializer)
    props.put("compression.type", compressionType)

    var i = 0
    val producer = new KafkaProducer[String, String](props)

    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    for (file <- getListOfFiles(dataDir).sorted) {
      val source: BufferedSource = io.Source.fromFile(file.getAbsoluteFile)
      println(file.getAbsoluteFile)
      for (line <- source.getLines) {

        val startTime = System.nanoTime()
        // TODO: we have to consider the kafka partition by changing key value
        val record = new ProducerRecord[String, String](topic, i.toString, line)
        producer.send(record)
        i += 1
        println(s"submit : $i, $line")
        val endTime = System.nanoTime()

        val elapsedTimeInSecond = (endTime - startTime) / 1000000

        if (msgInterval - elapsedTimeInSecond > 0) {
          sleep(msgInterval - elapsedTimeInSecond)
        }
      }
      source.close()
    }

    producer.close()
  }
}
