package kvstore

import akka.actor.{Props, ActorRef, Actor}
import kvstore.Arbiter._
import org.slf4j._
import kvstore.Persistence.{Persisted, Persist}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class RetryPersist(key: String, optOfValue: Option[String], id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(classOf[Replica], arbiter, persistenceProps)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._

  arbiter ! Join

  private val persistance = context.actorOf(persistenceProps)
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  private val logger = LoggerFactory.getLogger(this.getClass)

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var requesters = Map.empty[(String, Long), ActorRef]

  var done = Set.empty[Persisted]

  private var repExpectedSeqNo: Long = 0L

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def lookup(key: String): Option[String] = if (kv.isDefinedAt(key)) Some(kv(key)) else None

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = kv + ((key, value));
      sender ! OperationAck(id);
    }
    case Remove(key, id) => {
      kv = kv - key;
      sender ! OperationAck(id);
    }
    case Get(key, id) => sender ! GetResult(key, lookup(key), id)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) => {
      sender ! GetResult(key, lookup(key), id)
    }

    case Snapshot(key, valueOption, seq) => {
      if (seq < repExpectedSeqNo) {
        sender ! SnapshotAck(key, seq)
      } else if (seq > repExpectedSeqNo) {
        //ignore
      } else {
        valueOption match {
          case Some(value) => {
            kv = kv + ((key, value));
          }
          case None => {
            kv = kv - key;
          }
        }
        requesters = requesters + (((key, seq), sender))
        persistance ! Persist(key, valueOption, seq)
        context.system.scheduler.scheduleOnce(50 milliseconds, self, RetryPersist(key, valueOption, seq))
        repExpectedSeqNo = repExpectedSeqNo + 1
      }
    }

    case msg@Persisted(key, id) => {
      val originalRequester = requesters((key, id))
      done = done + msg
      originalRequester ! SnapshotAck(key, id)
    }

    case msg@RetryPersist(key, optOfValue, id) => {
      val persistMsg = Persist(key, optOfValue, id)
      if(!done.contains(Persisted(key, id))) {
        persistance ! persistMsg
        context.system.scheduler.scheduleOnce(50 milliseconds, self, msg)
      }
    }

  }

}
