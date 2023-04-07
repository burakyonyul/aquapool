import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import querying.actor.HashJoinPerformer
import querying.message.PerformHashJoin

class HashJoinPerformerTest extends TestKit(ActorSystem("HashJoinPerformerTest")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Hash Join Performer Actor" must {

    "perform hash join and return join result back to its sender" in {

      val probe = TestProbe()

      // create a new actor
      val hjp = system.actorOf(Props(new HashJoinPerformer))
      // watch created actor
      probe watch hjp
      // import first result
      val dbpediaResult = TestUtils.importJsonResult(TestUtils.DBPEDIA_JOIN_RESULT_NAME, 1)
      // import second result
      val lmdbResult = TestUtils.importJsonResult(TestUtils.LMDB_JOIN_RESULT_NAME, 1)
      // send perform hash join message
      hjp ! PerformHashJoin(dbpediaResult, lmdbResult)
      // import expected join result
      val expectedJoinResult = TestUtils.importJsonResult(TestUtils.DBPEDIA_LMDB_JOIN_RESULT_NAME, 1)
      // assert if expected result is same with actual result
      expectMsg(expectedJoinResult)
      // kill actor instance
      hjp ! PoisonPill
      // assert if actor instance has been terminated
      probe.expectTerminated(hjp)

    }
  }

}
