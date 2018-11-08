import scala.math._

object IsBot {
  def classification(clicks: Long, views: Long, categories: Set[Long]): Boolean = {
    return ((clicks + views) > 1000) ||
      ((clicks / math.max(views, 1)) > 5) ||
      (categories.size > 5)
  }
}