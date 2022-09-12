package part3_clustering

import akka.actor.{Actor, ActorLogging, ActorSystem, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory

class ClusterSubscriber extends Actor with ActorLogging {

  /*
    Build distributed applications:
    - decentralized, peer-to-peer
    - no single point of failure
    - automatic node membership and gossip protocol
    - failure detector

    Clustering is based on Remoting
    - in most cases, use Clustering instead of Remoting
   */

  /*
    Clusters
    Composed of member nodes:
    - node = host + port + UID
    - on the same JVM
    - on multiple JVMs on the same machine
    - on a set of machines of any scale

    Cluster membership:
    - convergent gossip protocol
    - Phi accrual failure detector - same as Remoting
    - no leader election - leader is deterministically chosen
   */

  /*
    Join a Cluster
    1. Contact seed nodes in order (from configuration)
      - If I am the first node, I'll join myself
      - Send a Join command to the seed node that respond first
    2. Node is in the "joining" state
      - wait for gossip to converge
      - all nodes in the cluster must acknowledge the new node
    3. The leader will set the state of the new node to "up"
   */

  /*
    Leave a cluster
    Option 1: safe and quiet
    - node switches its state to "leaving"
    - gossip converges
    - leader sets the state to "exiting"
    - gossip converges
    - leader marks it removed

    Option 2: the hard way
    - a node becomes unreachable
    - gossip converges and leader actions are not possible
    - must be removed (downed) manually
    - can also be auto-downed by the leader

    DO NOT use automatic downing in production
   */

  /*
    When to use Clustering
    - in a distributed application
    - within microservices
   */

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent],
      classOf[UnreachableMember]
    )
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case MemberJoined(member) =>
      log.info(s"New member in town: ${member.address}")
    case MemberUp(member) if member.hasRole("numberCruncher") =>
      log.info(s"HELLO BROTHER: ${member.address}")
    case MemberUp(member) =>
      log.info(s"Let's say welcome to the newest member: ${member.address}")
    case MemberRemoved(member, previousStatus) =>
      log.info(s"Poor ${member.address}, it was removed from $previousStatus")
    case UnreachableMember(member) =>
      log.info(s"Uh oh, member ${member.address} is unreachable")
    case m: MemberEvent =>
      log.info(s"Another member event: $m")
  }
}

object ClusteringBasics extends App {

  def startCluster(ports: List[Int]): Unit = ports.foreach { port =>
      val config = ConfigFactory.parseString(
        s"""
           |akka.remote.artery.canonical.port = $port
         """.stripMargin)
        .withFallback(ConfigFactory.load("part3_clustering/clusteringBasics.conf"))

      val system = ActorSystem("RTJVMCluster", config) // all the actor systems in a cluster must have the same name
      system.actorOf(Props[ClusterSubscriber], "clusterSubscriber")

      Thread.sleep(2000)
    }

  startCluster(List(2552, 2551, 0))
}

object ClusteringBasics_ManualRegistration extends App {
  val system = ActorSystem(
    "RTJVMCluster",
    ConfigFactory
      .load("part3_clustering/clusteringBasics.conf")
      .getConfig("manualRegistration")
  )

  val cluster = Cluster(system) // Cluster.get(system)

  def joinExistingCluster =
    cluster.joinSeedNodes(List(
      Address("akka", "RTJVMCluster", "localhost", 2551), // akka://RTJVMCluster@localhost:2551
      Address("akka", "RTJVMCluster", "localhost", 2552)  // equivalent with AddressFromURIString("akka://RTJVMCluster@localhost:2552")
    ))

  def joinExistingNode =
    cluster.join(Address("akka", "RTJVMCluster", "localhost", 50466))

  def joinMyself =
    cluster.join(Address("akka", "RTJVMCluster", "localhost", 2555))

  joinExistingCluster

  system.actorOf(Props[ClusterSubscriber], "clusterSubscriber")

}
