package kvstore

import org.specs2.mutable.Specification
import akka.actor.Props
import org.specs2.time.NoTimeConversions
import scala.concurrent.duration._
import Arbiter._
import Replica._
import Replicator._
import FosterParent._
import akka.testkit._

class ReplicatorSpec extends Specification with NoTimeConversions {
  sequential

  "Replicator" should {

    "call insert on secondary replica given insert with some value" in new AkkaTestkitSpecs2Support {
      val secondaryReplica = TestProbe()
      val replicator = system.actorOf(Replicator.props(secondaryReplica.ref))

      replicator ! Replicate("1", Some("one"), 1)
      secondaryReplica.expectMsgType[Snapshot] must be equalTo Snapshot("1", Some("one"), 0)
    }

    "call insert on secondary replica given insert with no value" in new AkkaTestkitSpecs2Support {
      val secondaryReplica = TestProbe()
      val replicator = system.actorOf(Replicator.props(secondaryReplica.ref))

      replicator ! Replicate("1", None, 1)
      secondaryReplica.expectMsgType[Snapshot] must be equalTo Snapshot("1", None, 0)
    }

    "retransmit snapshot if no acknowledgement" in new AkkaTestkitSpecs2Support {
      val secondaryReplica = TestProbe()
      val replicator = system.actorOf(Replicator.props(secondaryReplica.ref))

      replicator ! Replicate("1", None, 1)
      secondaryReplica.expectMsgType[Snapshot] must be equalTo Snapshot("1", None, 0)
      secondaryReplica.expectMsg(200 milliseconds, Snapshot("1", None, 0))
      secondaryReplica.expectMsg(200 milliseconds, Snapshot("1", None, 0))
    }

    /*"should not retransmit if acknowledgement was sent" in new AkkaTestkitSpecs2Support {
        1 == 1
    }*/

  }
}