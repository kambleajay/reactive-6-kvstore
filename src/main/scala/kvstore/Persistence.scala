package kvstore

import akka.actor._
import scala.util.Random

object Persistence {

  case class Persist(key: String, valueOption: Option[String], id: Long)

  case class Persisted(key: String, id: Long)

  class PersistenceException extends Exception("Persistence failure")

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor {

  import Persistence._

  var requests = Map.empty[Persist, ActorRef]

  override def preRestart(ex: Throwable, message: Option[Any]) {
    for((msg, sender) <- requests) {
      sender ! Persisted(msg.key, msg.id)
    }
  }

  def receive = {
    case msg@Persist(key, _, id) => {
      requests = requests + (msg -> sender)
      //println(s"\t\t\t[PS] Persist=$msg")
      if (!flaky || Random.nextBoolean()) {
        sender ! Persisted(key, id)
        requests = requests - (msg)
      }
      else {
        throw new PersistenceException
      }
    }
  }

}
