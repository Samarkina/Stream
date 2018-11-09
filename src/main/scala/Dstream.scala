import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.dstream.DStream

object Dstream {

  def main(): Unit = {


    val conf = new SparkConf().setAppName("stream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    ssc.sparkContext.setLogLevel("WARN")
    val topics = List("kafkaConnectStandalone")
    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    dstream
      .map(record => (record.key, record.value))
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
