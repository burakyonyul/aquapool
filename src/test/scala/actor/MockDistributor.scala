package actor

import akka.actor.Props
import querying.actor.Distributor
import querying.message.ExecuteServiceClause

class MockDistributor extends Distributor {
  override protected def distribute(query: String, endpoints: Seq[String]): Unit = {
    endpoints foreach {
      endpoint =>
        val exe = context.system.actorOf(Props(new MockExecutor))
        exe ! ExecuteServiceClause(query, endpoint)
    }
  }
}
