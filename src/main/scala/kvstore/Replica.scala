package kvstore

import akka.actor._
import kvstore.Arbiter._
import org.slf4j._
import kvstore.Persistence.{ PersistenceException, Persisted, Persist }
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import kvstore.Persistence.Persist
import scala.Some
import akka.actor.OneForOneStrategy
import kvstore.Persistence.Persisted

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

  case class PersistCheck(key: String, id: Long)

  case class GlobalAckCheck(key: String, id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(classOf[Replica], arbiter, persistenceProps)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._

  arbiter ! Join

  // override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy(maxNrOfRetries = 15) {
  //   case ex: PersistenceException => {
  //     println("!persistence exception")
  //     SupervisorStrategy.Resume
  //   }
  // }

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

  var donePersistence = Set.empty[Persisted]
  var failedPersistence = Set.empty[Persisted]
  //from operation to replicator who have to acknowledge
  var expectedAcks = Map.empty[(String, Long), Set[ActorRef]]

  private var repExpectedSeqNo: Long = 0L

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def lookup(key: String): Option[String] = if (kv.isDefinedAt(key)) Some(kv(key)) else None

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {

    case msg @ Insert(key, value, id) => {
      println(s"[P] Insert=$msg")
      kv = kv + ((key, value))
      requesters = requesters + (((key, id), sender))
      persistance ! Persist(key, Some(value), id)
      replicators foreach (_ ! Replicate(key, Some(value), id))
      expectedAcks = expectedAcks + ((key, id) -> replicators)
      context.system.scheduler.scheduleOnce(100 milliseconds, self, RetryPersist(key, Some(value), id))
      context.system.scheduler.scheduleOnce(1 second, self, PersistCheck(key, id))
      context.system.scheduler.scheduleOnce(1 second, self, GlobalAckCheck(key, id))
    }

    case msg @ Remove(key, id) => {
      println(s"[P] Remove=$msg")
      kv = kv - key
      requesters = requesters + (((key, id), sender))
      persistance ! Persist(key, None, id)
      replicators foreach (_ ! Replicate(key, None, id))
      context.system.scheduler.scheduleOnce(100 milliseconds, self, RetryPersist(key, None, id))
      context.system.scheduler.scheduleOnce(1 second, self, PersistCheck(key, id))
      context.system.scheduler.scheduleOnce(1 second, self, GlobalAckCheck(key, id))
    }

    case msg @ Persisted(key, id) => {
      println(s">>[P] Persisted=$msg")
      val originalRequester = requesters((key, id))
      donePersistence = donePersistence + msg
      val replAcks = expectedAcks.get(((key, id)))
      if (replicators.isEmpty || (replAcks.isDefined && replAcks.get.isEmpty)) {
        originalRequester ! OperationAck(id)
      }
    }

    case msg @ Replicated(key, id) => {
      println(s">>[P] Replicated=$msg")
      val acks = expectedAcks.get((key, id))
      if (acks.isDefined) {
        expectedAcks = expectedAcks + ((key, id) -> (acks.get - sender))
      }
      val isPersisted = donePersistence.contains(Persisted(key, id))
      val updatedAcks = expectedAcks.get((key, id))
      //println(s"STATS - acks size=${expectedAcks(((key, id))).size}, persisted=$isPersisted, rep size=${replicators.size}}")
      if ((!updatedAcks.isDefined || updatedAcks.get.isEmpty) && isPersisted) {
        val originalRequester = requesters((key, id))
        originalRequester ! OperationAck(id)
      }
    }

    case msg @ RetryPersist(key, optOfValue, id) => {
      println(s"[P] RetryPersist=$msg")
      val persistMsg = Persist(key, optOfValue, id)
      if (!donePersistence.contains(Persisted(key, id)) ||
        failedPersistence.contains(Persisted(key, id))) {
        persistance ! persistMsg
        context.system.scheduler.scheduleOnce(100 milliseconds, self, msg)
      }
    }

    case msg @ PersistCheck(key, id) => {
      println(s"[P] PersistCheck=$msg")
      if (!donePersistence.contains(Persisted(key, id))) {
        failedPersistence = failedPersistence + Persisted(key, id)
        val originalRequester = requesters((key, id))
        originalRequester ! OperationFailed(id)
      }
    }

    case msg @ Replicas(replicas) => {
      println(s"[P] Replicas=$msg")
      val secondaryReplicas = replicas - self
      secondaryReplicas.foreach { aReplica =>
        val lookup = secondaries.get(aReplica)
        if (!lookup.isDefined) {
          val aReplicator = context.actorOf(Replicator.props(aReplica))
          secondaries = secondaries + ((aReplica, aReplicator))
          replicators = replicators + aReplicator
          //replay all values
          for ((aKey, aVal) <- kv) {
            aReplicator ! Replicate(aKey, Some(aVal), System.currentTimeMillis())
          }
        }
      }

      val deadReplicas = secondaries.keySet diff replicas
      deadReplicas foreach { aDeadReplica =>
        val obsoleteReplicator = secondaries(aDeadReplica)
        context.stop(obsoleteReplicator)
        replicators = replicators - obsoleteReplicator
        secondaries = secondaries - aDeadReplica

        for (((key, id), repSet) <- expectedAcks) {
          if (repSet.contains(obsoleteReplicator)) {
            val validSet = repSet - obsoleteReplicator
            expectedAcks = expectedAcks + ((key, id) -> validSet)
            val isPersisted = donePersistence.contains(Persisted(key, id))
            if (validSet.isEmpty && isPersisted) {
              val originalRequester = requesters((key, id))
              originalRequester ! OperationAck(id)
            }
          }
        }
      }
    }

    case msg @ GlobalAckCheck(key, id) => {
      println(s"[P] GlobalAckCheck=$msg")
      if (!replicators.isEmpty) {
        val acks = expectedAcks.get(((key, id)))
        acks match {
          case Some(xs) if xs.isEmpty => ()
          case _ => {
            val originalRequester = requesters((key, id))
            originalRequester ! OperationFailed(id)
          }
        }
      }
    }

    case Get(key, id) => sender ! GetResult(key, lookup(key), id)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case msg @ Get(key, id) => {
      println(s"\t\t[S] Get=$msg")
      sender ! GetResult(key, lookup(key), id)
    }

    case msg @ Snapshot(key, valueOption, seq) => {
      println(s"\t\t[S] Snapshot=$msg")
      if (seq < repExpectedSeqNo) {
        sender ! SnapshotAck(key, seq)
      } else if (seq > repExpectedSeqNo) {
        //ignore
      } else {
        valueOption match {
          case Some(value) => {
            kv = kv + ((key, value))
          }
          case None => {
            kv = kv - key
          }
        }
        requesters = requesters + (((key, seq), sender))
        persistance ! Persist(key, valueOption, seq)
        context.system.scheduler.scheduleOnce(100 milliseconds, self, RetryPersist(key, valueOption, seq))
        repExpectedSeqNo = repExpectedSeqNo + 1
      }
    }

    case msg @ Persisted(key, id) => {
      println(s"\t\t[S] Persisted=$msg")
      val originalRequester = requesters((key, id))
      donePersistence = donePersistence + msg
      originalRequester ! SnapshotAck(key, id)
    }

    case msg @ RetryPersist(key, optOfValue, id) => {
      println(s"\t\t[S] RetryPersist=$msg")
      val persistMsg = Persist(key, optOfValue, id)
      if (!donePersistence.contains(Persisted(key, id))) {
        persistance ! persistMsg
        context.system.scheduler.scheduleOnce(100 milliseconds, self, msg)
      }
    }

  }

}
