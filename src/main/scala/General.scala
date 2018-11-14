import java.sql.Timestamp

import io.circe.Decoder
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import io.circe.parser._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._

case class Message(unix_time: Long, category_id: Long, ip: String, typeID: String)

case class TransformedMessage(unix_time: Timestamp, category_id: Long, ip: String, clicks: Long, views: Long)

case class TransformedMessageLong(unix_time: Long, category_id: Long, ip: String, clicks: Long, views: Long)

case class IsBotValue(isBot: Boolean, value: TransformedMessageValue)

case class TransformedMessageValue(categories: Set[Long], clicks: Long, views: Long) {
  def +(other: TransformedMessageValue): TransformedMessageValue = {
    TransformedMessageValue(categories ++ other.categories, clicks + other.clicks, views + other.views)
  }
}

case class AggregatedMessage(categories: Set[Long], ip: String, clicks: Long, views: Long)

object Message {
  implicit val decoder: Decoder[Message] =
    Decoder.forProduct4("unix_time", "category_id", "ip", "typeID")(Message.apply)

  def transform(message: Message): TransformedMessageLong = {
    if (message.typeID == "click")
      TransformedMessageLong(message.unix_time, message.category_id, message.ip, 1, 0)
    else
      TransformedMessageLong(message.unix_time, message.category_id, message.ip, 0, 1)
  }

  def transformedToTimestamp(message: TransformedMessageLong): TransformedMessage = {
    TransformedMessage(new Timestamp(message.unix_time), message.category_id, message.ip, message.clicks, message.views)
  }
}

object IsBotValue {
  def classification(value: List[TransformedMessageValue]): IsBotValue = {
    val aggregation = value.reduce((l, r) => (l + r))
    val isBot = IsBot.classification(aggregation.clicks, aggregation.views, aggregation.categories)
    IsBotValue(isBot, aggregation)
  }
}

object DstreamGeneral {

  def transformToValue(str: TransformedMessageLong): ((String, Long), TransformedMessageValue) = {
    ((str.ip, str.unix_time), TransformedMessageValue(Set(str.category_id), str.clicks, str.views))
  }

  def createDStream(ssc: StreamingContext): DStream[Message] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    ssc.sparkContext.setLogLevel("WARN")
    val topics = List("kafkaConnectStandalone")

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
      .flatMap(str => {
        val msg = decode[Message](str.value())
        if (msg.isRight)
          Some(msg.right.get)
        else
          None
      })

  }
}

