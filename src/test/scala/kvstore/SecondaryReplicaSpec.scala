package kvstore

import org.specs2.mutable.Specification
import akka.actor.Props
import org.specs2.time.NoTimeConversions
import scala.concurrent.duration._
import Arbiter._
import Replica._
import Replicator._

class SecondaryReplicaSpec extends Specification with NoTimeConversions {
  sequential

  "Secondary Replica" should {

    "handle snapshot message with expected seq no" in new AkkaTestkitSpecs2Support {
      val secondaryReplica = system.actorOf(Replica.props(self, Persistence.props(false)))

      expectMsgType[Join.type] must be equalTo Join
      secondaryReplica ! JoinedSecondary

      secondaryReplica ! Snapshot("1", Some("one"), 0)
      expectMsgType[SnapshotAck] must be equalTo SnapshotAck("1", 0)

      secondaryReplica ! Get("1", 2)
      expectMsgType[GetResult] must be equalTo GetResult("1", Some("one"), 2)
    }

    "not respond when seq no greater than expected" in new AkkaTestkitSpecs2Support {
      val secondaryReplica = system.actorOf(Replica.props(self, Persistence.props(false)))

      expectMsgType[Join.type] must be equalTo Join
      secondaryReplica ! JoinedSecondary

      secondaryReplica ! Snapshot("1", Some("one"), 1)
      expectNoMsg(100 milliseconds)

      secondaryReplica ! Get("1", 2)
      expectMsgType[GetResult] must be equalTo GetResult("1", None, 2)
    }

    "immediately respond and ignore when seq no less than expected" in new AkkaTestkitSpecs2Support {
      val secondaryReplica = system.actorOf(Replica.props(self, Persistence.props(false)))

      expectMsgType[Join.type] must be equalTo Join
      secondaryReplica ! JoinedSecondary

      secondaryReplica ! Snapshot("1", Some("one"), -1)
      expectMsgType[SnapshotAck] must be equalTo SnapshotAck("1", -1)

      secondaryReplica ! Get("1", 2)
      expectMsgType[GetResult] must be equalTo GetResult("1", None, 2)
    }

  }
}