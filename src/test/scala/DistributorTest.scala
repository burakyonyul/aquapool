import actor.MockDistributor
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.hp.hpl.jena.query.{ResultSet, ResultSetFactory}
import com.hp.hpl.jena.sparql.engine.binding.Binding
import com.hp.hpl.jena.sparql.resultset.ResultsFormat
import main.QueryIterCollection
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import querying.main.MonitoringUtils
import querying.message.{DistributeServiceClause, Result, ResultChange}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class DistributorTest extends TestKit(ActorSystem("DistributorTest")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Distributor actor" must {

    val dsc = DistributeServiceClause(TestUtils.PERSON_SELECT_QUERY, Vector(TestUtils.DBPEDIA_RESULT_FILE_NAME, TestUtils.IMDB_RESULT_FILE_NAME))
    "distribute a service clause and return result to the sender" in {
      val probe = TestProbe()
      // create a new actor
      val dist = system.actorOf(Props(new MockDistributor))
      probe watch dist
      dist ! dsc

      // check if received message is the expected one
      val expectedResult = generateExpectedResult(dsc.hashCode)
      expectMsg(expectedResult)
      // kill actor instance
      dist ! PoisonPill
      probe.expectTerminated(dist)
    }

    "notify change to its register list" in {
      TestUtils.changeFileInto(TestUtils.ACTUAL_DBPEDIA_RESULT_FILE_NAME, TestUtils.DBPEDIA_RESULT_FILE_NAME)
      TestUtils.changeFileInto(TestUtils.ACTUAL_IMDB_RESULT_FILE_NAME, TestUtils.IMDB_RESULT_FILE_NAME)
      val probe = TestProbe()

      // create changed result file by modifying actual result
      val changedJsonText = TestUtils.changeResultFile(TestUtils.IMDB_RESULT_FILE_NAME)

      // create a new actor
      val dist = system.actorOf(Props(new MockDistributor))
      probe watch dist
      dist ! dsc
      // check if received message is the expected one
      expectMsg(generateExpectedResult(dsc.hashCode))
      TestUtils.write2File(changedJsonText, TestUtils.IMDB_RESULT_FILE_NAME)
      // create expected message instance
      val resultChangeMsgExp = ResultChange(generateExpectedResult(dsc.hashCode),0)
      // check if received result is result change message
      expectMsg(10.seconds, resultChangeMsgExp)
      // kill actor instance
      dist ! PoisonPill
      probe.expectTerminated(dist)
    }

    "return same result if it is not changed and not notify any change" in {
      val probe = TestProbe()

      // arrange the result file as expected
      TestUtils.changeFileInto(TestUtils.ACTUAL_DBPEDIA_RESULT_FILE_NAME, TestUtils.DBPEDIA_RESULT_FILE_NAME)
      TestUtils.changeFileInto(TestUtils.ACTUAL_IMDB_RESULT_FILE_NAME, TestUtils.IMDB_RESULT_FILE_NAME)
      // create a new actor
      val dist = system.actorOf(Props(new MockDistributor))
      probe watch dist
      dist ! dsc

      //create expected message instance
      val expectedResult = generateExpectedResult(dsc.hashCode)
      expectMsg(expectedResult)
      // send same query again
      dist ! dsc
      // expect same result
      expectMsg(expectedResult)
      // kill actor instance
      dist ! PoisonPill
      probe.expectTerminated(dist)
    }
  }

  private def generateExpectedResult(expectedKey: Int) = {
    val imdbRes = ResultSetFactory.load(TestUtils.IMDB_RESULT_FILE_NAME, ResultsFormat.FMT_RS_JSON)
    var bindings = generateBindings(imdbRes)
    val dbpediaRes = ResultSetFactory.load(TestUtils.DBPEDIA_RESULT_FILE_NAME, ResultsFormat.FMT_RS_JSON)
    bindings ++= generateBindings(dbpediaRes)
    val expectedRes = constructResult(bindings, dbpediaRes.getResultVars, expectedKey)
    expectedRes
  }

  private def constructResult(bindingList: Vector[Binding], resVars: java.util.List[String], expectedKey: Int) = {
    val finalResultSet = ResultSetFactory.create(new QueryIterCollection(bindingList.asJava), resVars)
    val jsonText = MonitoringUtils.convertRdf2Json(finalResultSet)
    Result(jsonText, finalResultSet.getResultVars.asScala, expectedKey)
    //MonitoringUtils.convertRdf2Result(finalResultSet)
  }

  def generateBindings(resultSet: ResultSet) = {
    var bindingList: Vector[Binding] = Vector.empty
    while (resultSet.hasNext)
      bindingList = bindingList :+ resultSet.nextBinding
    bindingList
  }

}
