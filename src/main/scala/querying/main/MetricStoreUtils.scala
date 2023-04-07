package querying.main

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MetricStoreUtils {

  def increaseActorCount: Unit = {
    MetricStore.incr(Constants.ACTOR_COUNT)
  }

  def decreaseActorCount: Unit = {
    MetricStore.decr(Constants.ACTOR_COUNT)
  }

  def incrementQueryCount(anyQuery: Any) = {
    Future {
      val storedQuery = MetricStore.get(anyQuery.hashCode)
      if (storedQuery.isEmpty) {
        MetricStore.set(anyQuery.hashCode, anyQuery)
        MetricStore.incr(Constants.QUERY_COUNT)
      }
    }
  }

  def deleteStore = {
    MetricStore.deleteStore
  }

}
