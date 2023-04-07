import actor.MockParallelJoinManager
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import querying.message.DistributeBuckets

class ParallelJoinManagerTest extends TestKit(ActorSystem("HashJoinPerformerTest")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Bucket Distributor Actor" must {

    "distribute buckets, collect sub results and merge them into one main result" in {

      val probe = TestProbe()

      // create a new actor
      val bd = system.actorOf(Props(new MockParallelJoinManager))
      // watch created actor
      probe watch bd
      // import first result
      val dbpediaResult = TestUtils.importJsonResult(TestUtils.DBPEDIA_JOIN_RESULT_NAME,1)
      // import second result
      val lmdbResult = TestUtils.importJsonResult(TestUtils.LMDB_JOIN_RESULT_NAME, 1)
      // send perform hash join message
      bd ! DistributeBuckets(dbpediaResult, lmdbResult)
      // import expected join result
      val expectedJoinResult = TestUtils.importJsonResult(TestUtils.DBPEDIA_LMDB_JOIN_RESULT_NAME, 2)
      // assert if expected result is same with actual result
      expectMsg(expectedJoinResult)
      // kill actor instance
      bd ! PoisonPill
      // assert if actor instance has been terminated
      probe.expectTerminated(bd)

    }
  }

}
