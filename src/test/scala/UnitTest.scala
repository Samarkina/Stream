import org.scalatest.FunSuite

class UnitTest extends FunSuite {
  test("Is it bot? (IsBot.classification). Categories count more than 5") {
    val mess = AggregatedMessage(Set(1001, 1002, 1003, 1004, 1005, 1006), "172.20.0.5", 50, 50)
    assert(IsBot.classification(mess.clicks, mess.views, mess.categories) === true)
  }

  test("Is it bot? (IsBot.classification). Clicks and views count more than 1000") {
    val clicks = 500
    val views = 501
    val mess = AggregatedMessage(Set(1001, 1002, 1003), "172.20.0.5", clicks, views)
    assert(IsBot.classification(mess.clicks, mess.views, mess.categories) === true)
  }

  test("Is it bot? (IsBot.classification). Clicks/views more than 5") {
    val clicks = 5000
    val views = 5
    val mess = AggregatedMessage(Set(1001, 1002, 1003), "172.20.0.5", clicks, views)
    assert(IsBot.classification(mess.clicks, mess.views, mess.categories) === true)
  }

  test("Is it bot? (IsBot.classification). Not a bot. " +
    "Categories count less than 5. " +
    "Clicks and views count less than 1000" +
    "Clicks/views less than 5") {
    val clicks = 50
    val views = 50
    val mess = AggregatedMessage(Set(1001, 1002, 1003), "172.20.0.5", clicks, views)
    assert(IsBot.classification(mess.clicks, mess.views, mess.categories) === false)
  }
}


