import actor.MockFederator
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.hp.hpl.jena.query.ResultSetFactory
import com.hp.hpl.jena.sparql.resultset.ResultsFormat
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import querying.ClientApp
import querying.main.MonitoringUtils
import querying.message.{FederateQuery, Result}

import scala.concurrent.duration._

class FederatorTest extends TestKit(ActorSystem("QueryDistributorTest")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Query Federator actor" must {

    "federate query and return result to its register list" in {

      TestUtils.changeFileInto(TestUtils.GEO_ORIGINAL_RESULT_NAME, TestUtils.GEO_JOIN_RESULT_NAME)

      val probe = TestProbe()

      // create a new actor
      val fed = system.actorOf(Props(new MockFederator))
      probe watch fed
      // send execute sub query message
      fed ! FederateQuery(ClientApp.GOOD_LOOKING_QUERY, ""+fed.path.toString)

      //create expected message instance
      val expectedResult = createExpectedResult(TestUtils.DBPEDIA_LMDB_GEO_JOIN_RESULT_NAME, 1)
      // check if received message is the expected one
      expectMsg(expectedResult)
      // kill actor instance
      fed ! PoisonPill
      probe.expectTerminated(fed)
    }

    "notify change to its register list" in {
      TestUtils.changeFileInto(TestUtils.GEO_ORIGINAL_RESULT_NAME, TestUtils.GEO_JOIN_RESULT_NAME)
      val probe = TestProbe()

      // create a new actor
      val fed = system.actorOf(Props(new MockFederator))
      probe watch fed
      // send execute sub query message
      fed ! FederateQuery(ClientApp.GOOD_LOOKING_QUERY, fed.path.toString)

      //create expected message instance
      val expectedResult = createExpectedResult(TestUtils.DBPEDIA_LMDB_GEO_JOIN_RESULT_NAME, 1)
      // check if received message is the expected one
      expectMsg(expectedResult)

      TestUtils.changeFileInto(TestUtils.GEO_CHANGED_RESULT_NAME, TestUtils.GEO_JOIN_RESULT_NAME)
      // create expected message instance
      val expectedChangedResult = createExpectedResult(TestUtils.TRIPLE_JOIN_CHANGED_RESULT_NAME, 1)
      // check if received result is result change message
      expectMsg(10.seconds, expectedChangedResult)
      // kill actor instance
      fed ! PoisonPill
      probe.expectTerminated(fed)
    }

    "return same result if it is not changed and not notify any change" in {
      TestUtils.changeFileInto(TestUtils.GEO_ORIGINAL_RESULT_NAME, TestUtils.GEO_JOIN_RESULT_NAME)
      val probe = TestProbe()

      // create a new actor
      val fed = system.actorOf(Props(new MockFederator))
      probe watch fed
      // send execute sub query message
      fed ! FederateQuery(ClientApp.GOOD_LOOKING_QUERY, fed.path.toString)

      //create expected message instance
      val expectedResult = createExpectedResult(TestUtils.DBPEDIA_LMDB_GEO_JOIN_RESULT_NAME, 1)
      expectMsg(expectedResult)
      // send same query again
      fed ! FederateQuery(ClientApp.GOOD_LOOKING_QUERY, fed.path.toString)
      // expect same result
      expectMsg(expectedResult)
      // kill actor instance
      fed ! PoisonPill
      probe.expectTerminated(fed)
    }


  }

  private def createExpectedResult(endpoint: String, expectedKey: Int) = {
    val rsExp = ResultSetFactory.load(endpoint, ResultsFormat.FMT_RS_JSON)
    val result = MonitoringUtils.convertRdf2Result(rsExp)
    Result(result.resultJSON, result.resultVars, expectedKey)
  }
}
