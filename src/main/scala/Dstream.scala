import java.sql.Timestamp

import StructStream.aggregateDF
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.dstream.DStream



object Dstream {

  def mappingFunction(key: (String, Long), value: Option[TransformedMessageValue], state: State[List[(TransformedMessageValue, Long)]]): (String, List[TransformedMessageValue]) = {
    // Use state.exists(), state.get(), state.update() and state.remove()
    // to manage state, and return the necessary string
    val transformedMessageValue = value.getOrElse(TransformedMessageValue(Set.empty[Long], 0, 0))
    if (!state.isTimingOut) {
      state.update(
        if (state.exists) {
         state.get.filter(
           x => x._2 < (key._2 - 600)
         ) :+ (transformedMessageValue, key._2)
        }
        else {
          List((transformedMessageValue, key._2))
        }
      )
    }
    (key._1, state.get.map(x => x._1))
  }

  def aggregateDStream(transformedKeyValue: DStream[((String, Long), TransformedMessageValue)], spark: SparkSession): DStream[(String, IsBotValue)] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    transformedKeyValue
      .reduceByKeyAndWindow((a:TransformedMessageValue, b:TransformedMessageValue) => a + b, Minutes(10), Seconds(60))
      .mapWithState(
        StateSpec
          .function(mappingFunction _)
          .timeout(Minutes(10))
      )
      .reduceByKey((l: List[TransformedMessageValue], r: List[TransformedMessageValue]) => l ++ r)
      .mapValues(IsBotValue.classification)
      .filter(x => x._2.isBot)
  }

  def writeToCassandra(bots: DStream[(String, IsBotValue)], spark: SparkContext): Unit = {
    import com.datastax.spark.connector.streaming._
    bots
        .map(x => Tuple1(x._1))
        .saveToCassandra(
        "streamingdb",
        "botsdstream",
         SomeColumns("ip"),
          writeConf = WriteConf(
          ttl = TTLOption.constant(600),
          ifNotExists = true
        )
      )
  }


  def main(): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val dstream = DstreamGeneral.createDStream(ssc)

    val transformed = dstream
      .map(mess => Message.transform(mess))

    val transformedKeyValue = transformed
    .map(mess => DstreamGeneral.transformToValue(mess))

    val aggregated = aggregateDStream(transformedKeyValue, spark)

    val bots = aggregated
        .map(x => x._1)

    bots
      .print()

     writeToCassandra(
       aggregated,
       spark.sparkContext
     )

    ssc.checkpoint("/tmp/sparkCheckpoint")
    ssc.remember(Seconds(35))
    ssc.start()
    ssc.awaitTermination()

  }
}
