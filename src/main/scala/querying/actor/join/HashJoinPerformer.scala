package querying.actor.join

import akka.actor.{Actor, ActorLogging, Props}
import com.hp.hpl.jena.query.ResultSetFormatter
import main.ResultSetMerger
import play.api.libs.json.Json
import querying.message.{PerformHashJoin, Result}

import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._

object HashJoinPerformer {
  /*
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case phj@PerformHashJoin(_, _) => (phj.hashCode.toString, phj)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case phj@PerformHashJoin(_, _) => (phj.hashCode % numberOfShards).toString
  }
*/
  def props: Props = Props(new HashJoinPerformer)
}

class HashJoinPerformer extends Actor with ActorLogging {

  override def preStart(): Unit = {
    super.preStart
    //MetricStoreUtils.increaseActorCount
  }

  override def postStop(): Unit = {
    super.postStop
    //MetricStoreUtils.decreaseActorCount
  }

  override def receive: Receive = {
    case PerformHashJoin(firstRs, secondRs) =>
      val resultSet = new ResultSetMerger().mergeResultSets(firstRs.toResultSet, secondRs.toResultSet)
      //serialize result set
      val outputStream = new ByteArrayOutputStream
      ResultSetFormatter.outputAsJSON(outputStream, resultSet)
      //send hash join result back end the sender
      sender ! Result(Json.parse(outputStream.toByteArray), resultSet.getResultVars.asScala, 1)
      context.stop(self)
      //context.parent ! ShardRegion.Passivate(stopMessage = PoisonPill)
/*
    case ShardRegion.Passivate =>
      log.info("Passivation message has been received start parent shard!")
      context.stop(self)
*/
  }
}
