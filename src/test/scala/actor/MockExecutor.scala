package actor

import akka.actor.Cancellable
import akka.actor.TypedActor.dispatcher
import com.hp.hpl.jena.query.ResultSetFactory
import com.hp.hpl.jena.sparql.resultset.ResultsFormat
import querying.actor.Executor
import querying.main.MonitoringUtils
import querying.message.{Result, ScheduledServiceClause}

import scala.concurrent.duration._

class MockExecutor extends Executor {

  override protected def executeQuery(query: String, store: String): Result = {
    val res = ResultSetFactory.load(store, ResultsFormat.FMT_RS_JSON)
    val resultMock = MonitoringUtils.convertRdf2Result(res)
    Result(resultMock.resultJSON, resultMock.resultVars, store.hashCode)
  }

  override protected def schedule(ssc: ScheduledServiceClause): Cancellable = {
    context.system.scheduler.schedule(5.seconds, 5.seconds, self, ssc)
  }
}
