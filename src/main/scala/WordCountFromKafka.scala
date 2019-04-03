import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._

object WordCountFromKafka extends App {

  val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  ).asJava

  val offsetRanges = Array[OffsetRange](
    OffsetRange("test_topic", 0, 0, 10)
  )

  val kafkaRDD = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, LocationStrategies.PreferConsistent)
  val wordsRDD = kafkaRDD.map(r => r.value())

}
