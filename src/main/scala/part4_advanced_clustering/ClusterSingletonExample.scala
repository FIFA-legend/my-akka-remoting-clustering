package part4_advanced_clustering

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Random

/*
  How To Use Singleton
  Start the singleton: ClusterSingletonManager
  - ensures deployment of singleton
  - guarantees single instance per cluster
  - is started as early as possible, on every node

  The actual actor:
  - is a child of ClusterSingletonManager
  - runs on the oldest running node
  - is moved to a new oldest node, if the current one leaves (handover)

  Talk to singleton: ClusterSingletonProxy
  - knows where the singleton actually is
  - serves as a mediator to the singleton actor
  - buffering incoming messages during singleton handover
 */

/*
  A small payment service - CENTRALIZED
  - maintaining a well-defined transaction ordering
  - interacting with legacy systems
  - etc
 */

case class Order(items: List[String], total: Double)
case class Transaction(orderId: Int, txnId: String, amount: Double)

class PaymentSystem extends Actor with ActorLogging {
  override def receive: Receive = {
    case t: Transaction => log.info(s"Validating transaction $t") // add complex business logic here
    case m => log.info(s"Received unknown message: $m")
  }
}

class PaymentSystemNode(port: Int, shouldStartSingleton: Boolean = true) extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
     """.stripMargin)
    .withFallback(ConfigFactory.load("part4_advanced_clustering/clusterSingletonExample.conf"))

  val system = ActorSystem("RTJVMCluster", config)

  if (shouldStartSingleton)
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props[PaymentSystem],
        terminationMessage = PoisonPill,
        ClusterSingletonManagerSettings(system)
      ),
      "paymentSystem"
    )
}

object Node1 extends PaymentSystemNode(2551)
object Node2 extends PaymentSystemNode(2552)
object Node3 extends PaymentSystemNode(2553, false)

class OnlineShopCheckout(paymentSystem: ActorRef) extends Actor with ActorLogging {
  var orderId = 0

  override def receive: Receive = {
    case Order(_, totalAmount) =>
      log.info(s"Received order $orderId for amount $totalAmount, sending transaction to validate")
      val newTransaction = Transaction(orderId, UUID.randomUUID().toString, totalAmount)
      paymentSystem ! newTransaction
      orderId += 1
  }
}

object OnlineShopCheckout {
  def props(paymentSystem: ActorRef) = Props(new OnlineShopCheckout(paymentSystem))
}

object PaymentSystemClient extends App {
  val config = ConfigFactory.parseString(
    """
      |akka.remote.artery.canonical.port = 0
    """.stripMargin)
    .withFallback(ConfigFactory.load("part4_advanced_clustering/clusterSingletonExample.conf"))
  val system = ActorSystem("RTJVMCluster", config)

  val proxy = system.actorOf(
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/paymentSystem",
      settings = ClusterSingletonProxySettings(system)
    ),
    "paymentSystemProxy"
  )

  val onlineShopCheckout = system.actorOf(OnlineShopCheckout.props(proxy))

  import system.dispatcher
  system.scheduler.schedule(5.seconds, 1.second, () => {
    val randomOrder = Order(List(), Random.nextDouble() * 100)
    onlineShopCheckout ! randomOrder
  })

}

/*
  When to use it:
  - single entry point in an application
  - anything that needs to be centralized
  - single point of responsibility for certain decisions

  Benefits:
  - single instance guarantee
  - automatic handover

  Drawbacks
  - single point of bottleneck
  - no guaranteed 100% uptime during handover
 */

