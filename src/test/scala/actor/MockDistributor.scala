package actor

import akka.actor.Props
import querying.actor.Distributor
import querying.message.ExecuteQuery

class MockDistributor extends Distributor {
  override protected def distribute(query: String, stores: Seq[String]): Unit = {
    stores foreach {
      endpoint =>
        val exe = context.system.actorOf(Props(new MockExecutor))
        exe ! ExecuteServiceClause(query, endpoint)
    }
  }
}
