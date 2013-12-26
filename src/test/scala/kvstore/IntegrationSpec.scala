package kvstore

import org.specs2.mutable.Specification
import akka.actor.Props
import org.specs2.time.NoTimeConversions
import scala.concurrent.duration._
import Arbiter._
import Replica._

class IntegrationSpec extends Specification with NoTimeConversions {
  sequential

  "kvstore" should {

    "work" in new AkkaTestkitSpecs2Support {
      val primary = system.actorOf(Replica.props(self, Persistence.props(true)), "primary")
      expectMsgType[Join.type] must be equalTo Join
      primary ! JoinedPrimary

      val secondaryA = system.actorOf(Replica.props(self, Persistence.props(true)), "A")
      expectMsgType[Join.type] must be equalTo Join
      secondaryA ! JoinedSecondary

      val secondaryB = system.actorOf(Replica.props(self, Persistence.props(true)), "B")
      expectMsgType[Join.type] must be equalTo Join
      secondaryB ! JoinedSecondary

      val secondaryC = system.actorOf(Replica.props(self, Persistence.props(true)), "C")
      expectMsgType[Join.type] must be equalTo Join
      secondaryC ! JoinedSecondary      

      primary ! Replicas(Set(primary, secondaryA, secondaryB))

      primary ! Insert("a", "a", 1)
      expectMsgType[OperationAck] must be equalTo OperationAck(1)

      primary ! Insert("b", "b", 2)
      expectMsgType[OperationAck] must be equalTo OperationAck(2)

      primary ! Replicas(Set(primary, secondaryA))

      primary ! Insert("c", "c", 3)
      expectMsgType[OperationAck] must be equalTo OperationAck(3)

      //primary ! Replicas(Set(primary, secondaryB, secondaryC))

      //primary ! Remove("a", 4)
      //expectMsgType[OperationAck] must be equalTo OperationAck(4)
    }

  }
}