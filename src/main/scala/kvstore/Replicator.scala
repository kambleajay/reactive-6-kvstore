package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  case class RetrySnapshot(key: String, valueOption: Option[String], seq: Long)

  def props(replica: ActorRef): Props = Props(classOf[Replicator], replica)
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  var done = Vector.empty[Snapshot]

  var _seqCounter = 0L

  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case msg@Replicate(key, optOfValue, id) => {
      println(s"\t[RT] Replicate=$msg")
      val seqNo = nextSeq
      val snapshotMsg = Snapshot(key, optOfValue, seqNo)
      pending = pending :+ snapshotMsg
      acks = acks + ((seqNo, (sender, msg)))

      replica ! snapshotMsg
      context.system.scheduler.scheduleOnce(100 milliseconds, self, RetrySnapshot(key, optOfValue, seqNo))
    }

    case msg@SnapshotAck(inkey, inseq) => {
      println(s"\t[RT] SnapshotAck=$msg")
      val pendingSize = pending.size
      pending = pending.filterNot(snapshot => snapshot.key == inkey && snapshot.seq == inseq)
      assert((pending.size + 1) == pendingSize)
      val (sender, req) = acks(inseq)
      done = done :+ Snapshot(inkey, req.valueOption, inseq)
      sender ! Replicated(inkey, req.id)
    }

    case msg@RetrySnapshot(key, optOfValue, seq) => {
      println(s"\t[RT] RetrySnapshot=$msg")
      val snapshotMsg = Snapshot(key, optOfValue, seq)
      if (!done.contains(snapshotMsg)) {
        replica ! snapshotMsg
        context.system.scheduler.scheduleOnce(100 milliseconds, self, msg)
      }
    }

  }

}
