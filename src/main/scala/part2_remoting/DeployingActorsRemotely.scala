package part2_remoting

import akka.actor.{Actor, ActorLogging, ActorSystem, Address, AddressFromURIString, Deploy, PoisonPill, Props, Terminated}
import akka.remote.RemoteScope
import akka.routing.FromConfig
import com.typesafe.config.ConfigFactory

object DeployingActorsRemotely_LocalApp extends App {
  val system = ActorSystem("LocalActorSystem", ConfigFactory.load("part2_remoting/deployingActorsRemotely.conf").getConfig("localApp"))

  // This is deployed on a remote ActorSystem because configuration is set in application.conf
  val simpleActor = system.actorOf(Props[SimpleActor], "remoteActor") // /user/remoteActor
  simpleActor ! "hello, remote actor!"

  /*
    Remote Deployment
    Steps:
    - the name of the actor is checked in config for remote deployment
      - if it's not there, it will be created locally
    - the Props passed to actorOf() will be sent to the remote actor system
    - the remote actor system will create it there
    - an ActorRef is returned

    Caveat: the Props object needs to be serializable
    - in 99% of cases it is
    - watch for lambdas that need to be sent over the wire
    - the actor class needs to be in the remote JVM's classpath
   */

  // actor path of a remotely deployed actor
  println(simpleActor)
  // expected: akka://RemoteActorSystem@localhost:2552/user/remoteActor
  // actual: akka://RemoteActorSystem@localhost:2552/remote/akka/LocalActorSystem@localhost:2551/user/remoteActor

  // programmatic remote deployment
  val remoteSystemAddress: Address = AddressFromURIString("akka://RemoteActorSystem@localhost:2552")
  val remotelyDeployedActor = system.actorOf(
    Props[SimpleActor].withDeploy(
      Deploy(scope = RemoteScope(remoteSystemAddress))
    )
  )

  remotelyDeployedActor ! "hi, remotely deployed actor!"

  // routers with routees deployed remotely
  val poolRouter = system.actorOf(FromConfig.props(Props[SimpleActor]), "myRouterWithRemoteChildren")
  (1 to 10).map(i => s"message $i").foreach(poolRouter ! _)

  // watching remote actors
  class ParentActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case "create" =>
        log.info("Creating remote child")
        val child = context.actorOf(Props[SimpleActor], "remoteChild")
        context.watch(child)
      case Terminated(ref) =>
        log.warning(s"Child $ref terminated")
    }
  }

  val parentActor = system.actorOf(Props[ParentActor], "watcher")
  parentActor ! "create"
//  Thread.sleep(1000)
//  system.actorSelection("akka://RemoteActorSystem@localhost:2552/remote/akka/LocalActorSystem@localhost:2551/user/watcher/remoteChild") ! PoisonPill

  /*
    Failure Detector:
    - actor system send heartbeat messages once a connection is established
      - sending a message
      - deploying a remote actor
    - if a heartbeat times out, its reach score (PHI) increases
    - if a PHI score passes a threshold (10), the connection is quarantined = unreachable
    - the local actor system sends Terminated messages to Death Watchers of remote actors
    - the remote actor system must be restarted to reestablish connection
   */

}

object DeployingActorsRemotely_RemoteApp extends App {
  val system = ActorSystem("RemoteActorSystem", ConfigFactory.load("part2_remoting/deployingActorsRemotely.conf").getConfig("remoteApp"))
}