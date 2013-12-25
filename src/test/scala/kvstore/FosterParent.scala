package kvstore

import akka.actor._

object FosterParent {
  def props(childProps: Props, probe: ActorRef): Props = Props(new FosterParent(childProps, probe))
}

class FosterParent(childProps: Props, probe: ActorRef) extends Actor {
  val child = context.actorOf(childProps, "child")
  def receive: Receive = {
    case msg if sender == context.parent =>
      probe forward msg
      child forward msg
    case msg =>
      probe forward msg
      context.parent forward msg  
  }
}