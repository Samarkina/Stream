import java.sql.Timestamp

import io.circe.Decoder

case class Message(unix_time: Long, category_id: Long, ip: String, typeID: String)

case class TrasformedMessage(unix_time: Timestamp, category_id: Long, ip: String, clicks: Long, views: Long)

case class AggregatedMessage(categories: Set[Long], ip: String, clicks: Long, views: Long)

object Message {
  implicit val decoder: Decoder[Message] =
    Decoder.forProduct4("unix_time", "category_id", "ip", "typeID")(Message.apply)

  def transform(message: Message): TrasformedMessage = {
    if (message.typeID == "click")
      TrasformedMessage(new Timestamp(message.unix_time), message.category_id, message.ip, 1, 0)
    else
      TrasformedMessage(new Timestamp(message.unix_time), message.category_id, message.ip, 0, 1)
  }
}
