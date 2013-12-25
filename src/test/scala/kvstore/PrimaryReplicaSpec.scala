package kvstore

import org.specs2.mutable.Specification
import akka.actor.Props
import org.specs2.time.NoTimeConversions
import scala.concurrent.duration._
import Arbiter._
import Replica._

class PrimaryReplicaSpec extends Specification with NoTimeConversions {
  sequential

  "Primary Replica" should {

    "send join message to arbiter" in new AkkaTestkitSpecs2Support {
      val primaryReplica = system.actorOf(Replica.props(self, Persistence.props(false)))
      expectMsgType[Join.type] must be equalTo Join
    }

    "insert given element" in new AkkaTestkitSpecs2Support {
      val primaryReplica = system.actorOf(Replica.props(self, Persistence.props(false)))

      expectMsgType[Join.type] must be equalTo Join
      primaryReplica ! JoinedPrimary

      primaryReplica ! Insert("1", "one", 1)
      expectMsgType[OperationAck] must be equalTo OperationAck(1)

      primaryReplica ! Insert("2", "two", 2)
      expectMsgType[OperationAck] must be equalTo OperationAck(2)
    }

    "remove given element" in new AkkaTestkitSpecs2Support {
      val primaryReplica = system.actorOf(Replica.props(self, Persistence.props(false)))

      expectMsgType[Join.type] must be equalTo Join
      primaryReplica ! JoinedPrimary

      primaryReplica ! Remove("1", 1)
      expectMsgType[OperationAck] must be equalTo OperationAck(1)

      primaryReplica ! Insert("2", "two", 2)
      expectMsgType[OperationAck] must be equalTo OperationAck(2)

      primaryReplica ! Remove("2", 3)
      expectMsgType[OperationAck] must be equalTo OperationAck(3)            
    }

    "handle get request properly" in new AkkaTestkitSpecs2Support {
      val primaryReplica = system.actorOf(Replica.props(self, Persistence.props(false)))

      expectMsgType[Join.type] must be equalTo Join
      primaryReplica ! JoinedPrimary

      primaryReplica ! Get("1", 1)
      expectMsgType[GetResult] must be equalTo GetResult("1", None, 1)

      primaryReplica ! Insert("2", "two", 2)
      expectMsgType[OperationAck] must be equalTo OperationAck(2)

      primaryReplica ! Get("2", 3)
      expectMsgType[GetResult] must be equalTo GetResult("2", Some("two"), 3)      
    }

  }
}