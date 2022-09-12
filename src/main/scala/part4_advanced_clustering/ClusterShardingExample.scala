package part4_advanced_clustering

import java.util.{Date, UUID}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, ReceiveTimeout}
import akka.cluster.sharding.ShardRegion.Passivate
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.typesafe.config.ConfigFactory

import scala.util.Random
import scala.concurrent.duration._

/*
  We'll split an actor A in multiple smaller actors:
  - same type A
  - potentially multiple instances on the same node
  - an instance of A = entity, and it has an ID

  Every node starts Cluster Sharding for an actor type
  - every node starts a special Shard Region actor
  - every Shard Region is responsible for a Shard ID
  - a special Shard Coordinator starts as a cluster singleton

  Every message:
  - is sent to the Shard Region of the local node
  - the local Shard Region will map the message to a Shard ID and an Entity ID
  - the local Shard Region will ask the Shard Coordinator for the destination node
  - the Shard Region will forward the message to the current node and entity
 */

/*
  Magical Features
  Shard rebalancing:
  - if a shard has too many entities, the entities can be migrated to other nodes
  - during handover, messages for a shard are buffered
  - the state of the entities is not migrated

  Shard passivation:
  - if an entity is passive for a while, best to stop it to free its memory
  - entity sends Passivate message to its parent ShardRegion
  - ShardRegion stops the entity
  - ShardRegion creates new entity when needed
 */

/*
  When a Node crashes
  The messages will be handled by the other nodes
  - the coordinator will distribute the dead shards to the existing nodes
  - new entities will be created on demand
  - messages to those shards are buffered until consensus

  If the Shard Coordinator dies:
  - it's moved to another node (it's a Singleton)
  - during handover, all messages to the Coordinator are buffered
  - all messages requiring the Coordinator are buffered
 */

case class OysterCard(id: String, amount: Double)
case class EntryAttempt(oysterCard: OysterCard, date: Date)
case object EntryAccepted
case class EntryRejected(reason: String)
// passivate message
case object TerminateValidator

/////////////////////////////////
// Actors
/////////////////////////////////
object Turnstile {
  def props(validator: ActorRef) = Props(new Turnstile(validator))
}

class Turnstile(validator: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case o: OysterCard => validator ! EntryAttempt(o, new Date)
    case EntryAccepted => log.info("GREEN: please pass")
    case EntryRejected(reason) => log.info(s"RED: $reason")
  }
}

class OysterCardValidator extends Actor with ActorLogging {
  /*
    Store an enormous amount of data.
   */

  override def preStart(): Unit = {
    super.preStart()
    log.info("Validator starting")
    context.setReceiveTimeout(10.seconds)
  }

  override def receive: Receive = {
    case EntryAttempt(card @ OysterCard(id, amount), _) =>
      log.info(s"Validating $card")
      if (amount > 2.5) sender() ! EntryAccepted
      else sender() ! EntryRejected(s"[$id] not enough funds, please top up")
    case ReceiveTimeout =>
      context.parent ! Passivate(TerminateValidator)
    case TerminateValidator => // I am sure that I won't be contacted again, so safe to stop
      context.stop(self)
  }
}

/////////////////////////////////
// Sharding settings
/////////////////////////////////

object TurnstileSettings {

  val numberOfShards = 10 // use 10x number of nodes in your cluster
  val numberOfEntities = 100 // 10x number of shards

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case attempt @ EntryAttempt(OysterCard(cardId, _), _) =>
      val entityId = cardId.hashCode.abs % numberOfEntities
      (entityId.toString, attempt)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case EntryAttempt(OysterCard(cardId, _), _) =>
      val shardId = cardId.hashCode.abs % numberOfShards
      shardId.toString
    case ShardRegion.StartEntity(entityId) =>
      (entityId.toLong % numberOfShards).toString
  }

  /*
    There must be NO two messages M1 and M2 for which
    extractEntityId(M1) == extractEntityId(M2) and extractShardId(M1) != extractShardId(M2)

      OTHERWISE BAD. VERY BAD.

    entityId -> shardId, then FORALL messages M, if extractEntityId(M) = entityId, then extractShardId(M) MUST BE shardId
   */
}

/////////////////////////////////
// Cluster nodes
/////////////////////////////////

class TubeStation(port: Int, numberOfTurnstiles: Int) extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
     """.stripMargin)
    .withFallback(ConfigFactory.load("part4_advanced_clustering/clusterShardingExample.conf"))

  val system = ActorSystem("RTJVMCluster", config)

  // Setting up Cluster Sharding
  val validatorShardRegionRef: ActorRef = ClusterSharding(system).start(
    typeName = "OysterCardValidator",
    entityProps = Props[OysterCardValidator],
    settings = ClusterShardingSettings(system).withRememberEntities(true),
    extractEntityId = TurnstileSettings.extractEntityId,
    extractShardId = TurnstileSettings.extractShardId
  )

  val turnstiles = (1 to numberOfTurnstiles).map(_ => system.actorOf(Turnstile.props(validatorShardRegionRef)))

  Thread.sleep(10000)
  for (_ <- 1 to 1000) {
    val randomTurnstileIndex = Random.nextInt(numberOfTurnstiles)
    val randomTurnstile = turnstiles(randomTurnstileIndex)

    randomTurnstile ! OysterCard(UUID.randomUUID().toString, Random.nextDouble() * 10)
    Thread.sleep(200)
  }
}

object PiccadillyCircus extends TubeStation(2551, 10)
object Westminster extends TubeStation(2561, 5)
object CharingCross extends TubeStation(2571, 15)