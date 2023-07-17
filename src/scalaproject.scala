
import scala.collection.mutable
import scala.io.Source

case class Order(orderId: String, userName: String, orderTime: Long, orderType: String, price: Int, quantity: Int)

object  scalaproject{
  def main(args: Array[String]): Unit = {
  
 val orders = io.Source.fromFile("/scalaproject/src/datafile.csv")
  val matchedOrs = Seq(orders)

    matchedOrs.foreach(println)
  }

  def readOrdersFromFile(filePath: String): Seq[Order] = {
    val source = Source.fromFile(filePath)
    val lines = source.getLines().drop(1)
    val orders = mutable.ArrayBuffer.empty[Order]

    for (line <- lines) {
      val fields = line.split(",")
      val order = Order(
        orderId = fields(0),
        userName = fields(1),
        orderTime = fields(2).toLong,
        orderType = fields(3),
        quantity = fields(4).toInt,
        price = fields(5).toInt
      )
      orders += order
    }

    source.close()
    orders.toSeq
  }

  def matchOrders(orders: Seq[Order]): Seq[(Order, Order)] = {
    val ordersBuy = mutable.Queue.empty[Order]
    val ordersSell = mutable.Queue.empty[Order]
    val matchedOrs = mutable.ArrayBuffer.empty[(Order, Order)]

    for (order <- orders) {
      if (order.orderType == "BUY") {
        matchOrder(order, ordersSell, matchedOrs)
        if (order.quantity > 0) {
          ordersBuy.enqueue(order)
        }
      } else if (order.orderType == "SELL") {
        matchOrder(order, ordersBuy, matchedOrs)
        if (order.quantity > 0) {
          ordersSell.enqueue(order)
        }
      }
    }

    matchedOrs.toSeq
  }

  def matchOrder(order: Order, oppositeOrders: mutable.Queue[Order], matchedOrs: mutable.ArrayBuffer[(Order, Order)]): Unit = {
    while (order.quantity > 0 && oppositeOrders.nonEmpty) {
      val oppositeOrder = oppositeOrders.dequeue()
      if (order.price >= oppositeOrder.price) {
        val quantityMatched = Math.min(order.quantity, oppositeOrder.quantity)
        val matchedOrder = (order.copy(quantity = quantityMatched), oppositeOrder.copy(quantity = quantityMatched))
        matchedOrs += matchedOrder

       
        val order.quantity = order.quantity - quantityMatched
    
        val oppositeOrder.quantity = oppositeOrder.quantity - quantityMatched

        if (oppositeOrder.quantity > 0) {
          oppositeOrders.enqueue(oppositeOrder)
        }
      } else {
        oppositeOrders.enqueue(oppositeOrder)
      }
    }
  }
}