package actor

import akka.actor.Props
import querying.actor.ParallelJoinManager
import querying.actor.join.{HashJoinPerformer, ParallelJoinManager}
import querying.message.Result

class MockParallelJoinManager extends ParallelJoinManager {
  override protected def performDistribution(firstRes: Result, secondRes: Result): Unit = {
    val hjp = context.system.actorOf(Props(new HashJoinPerformer))
    performDistribution(firstRes, secondRes)
  }
}
