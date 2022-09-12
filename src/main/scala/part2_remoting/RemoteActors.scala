package part2_remoting

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, ActorSystem, Identify, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object RemoteActors extends App {

  /*
    Actor model principles:
    - every interaction based on sending messages
    - full actor encapsulation
    - no locking
    - message-sending latency
    - at most once message delivery
    - message ordering maintained per sender/receiver pair

    The principles hold:
    - on the same JVM in a parallel application
    - locally on multiple JVMs
    - in a distributed environment of any scale

    Akka was designed with the distributed goal in mind
   */

  /*
    Location Transparency
    We communicate with actors via their references
    Location transparency = the real actor can be anywhere:
    - on the same JVM
    - on a different JVM
    - half a planet apart

    Location transparency != transparent remoting
    - location transparency = we don't care where the object is
    - transparent remoting = we're using the object as if it was local
   */

  // Using Akka Remoting Artery

  val localSystem = ActorSystem("LocalSystem", ConfigFactory.load("part2_remoting/remoteActors.conf"))
  val localSimpleActor = localSystem.actorOf(Props[SimpleActor], "localSimpleActor")
  localSimpleActor ! "hello, local actor!"

  // send a message to the REMOTE simple actor

  // Method 1: actor selection
  val remoteActorSelection = localSystem.actorSelection("akka://RemoteSystem@localhost:2552/user/remoteSimpleActor")
  remoteActorSelection ! "hello from the \"local\" JVM"

  // Method 2: resolve the actor selection to an actor ref
  import localSystem.dispatcher
  implicit val timeout = Timeout(3 seconds)
  val remoteActorRefFuture = remoteActorSelection.resolveOne()
  remoteActorRefFuture.onComplete {
    case Success(actorRef) => actorRef ! "I've resolved you in a future!"
    case Failure(ex) => println(s"I failed to resolve the remote actor because: $ex")
  }

  // Method 3: actor identification via messages
  /*
    - actor resolver will ask for an actor selection from the local actor system
    - actor resolver will send a Identify(42) to the actor selection
    - the remote actor will AUTOMATICALLY respond with ActorIdentity(42, actorRef)
    - the actor resolver is free to use the remote actorRef
   */
  class ActorResolver extends Actor with ActorLogging {
    override def preStart(): Unit = {
      val selection = context.actorSelection("akka://RemoteSystem@localhost:2552/user/remoteSimpleActor")
      selection ! Identify(42)
    }

    override def receive: Receive = {
      case ActorIdentity(42, Some(actorRef)) =>
        actorRef ! "Thank you for identifying yourself!"
    }
  }

  localSystem.actorOf(Props[ActorResolver], "localActorResolver")
}

object RemoteActors_Remote extends App {
  val remoteSystem = ActorSystem("RemoteSystem", ConfigFactory.load("part2_remoting/remoteActors.conf").getConfig("remoteSystem"))
  val remoteSimpleActor = remoteSystem.actorOf(Props[SimpleActor], "remoteSimpleActor")
  remoteSimpleActor ! "hello, remote actor!"
}