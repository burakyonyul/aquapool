package querying

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import querying.actor.Agent
import querying.evaluation.Queries
import querying.message.{PolyStoreQuery, Result}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
 * TODO: Polystore use cases need to be generated
 */
object AgentApp {

  def main(args: Array[String]): Unit = {
    val ipAddress = if (args.isDefinedAt(0)) args(0) else getIpAddress
    val port = if (args.isDefinedAt(1)) args(1) else "2553"
    val queryPath = if (args.isDefinedAt(2)) args(2) else s"${System.getProperty("user.home")}/PolyStoreQuery.json"
    val config = ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname = $ipAddress").
      withFallback(ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")).
      withFallback(ConfigFactory.load("agent.conf"))

    // Create an Akka system
    val system = ActorSystem("ClientQuerier", config)

    val agent = system.actorOf(Agent.props, "QuerierClient-1")

    //val query_map = HashMap(Queries.REDIS_LRANGE_MAGNESIUM -> Constants.REDIS, Queries.INFLUXDB_ITEM_OF_SUBJECT -> Constants.INFLUXDB)
    //val query_map = HashMap(Queries.REDIS_LRANGE_SEPSIS_PATIENTS -> Constants.REDIS, Queries.ELASTICSEARCH_AZOTEMIA_PATIENTS -> Constants.ELASTICSEARCH)
    implicit val timeout: Timeout = Timeout(10.minutes)
    val future = agent ? PolyStoreQuery(Queries.Query_A_2_1, "1")
    val result = Await.result(future, timeout.duration)
    val resultSet = result.asInstanceOf[Result].toResultSet
    var rows = 0
    while (resultSet.hasNext) {
      resultSet.next()
      rows += 1
    }
    println(s"Row Count: $rows")

    //val polyStoreQuery = PolystoreQueryReader.read(queryPath)
    //println(polyStoreQuery)
    //agent ! polyStoreQuery

  }

  private def getIpAddress: String = {
    /*
    val e = NetworkInterface.getNetworkInterfaces
    if (e.hasMoreElements) {
      val n = e.nextElement match {
        case e: NetworkInterface => e
        case _ => ???
      }
      val ee = n.getInetAddresses
      if (ee.hasMoreElements) {
        ee.nextElement match {
          case e: InetAddress => return e.getHostAddress
          case _ => ???
        }
      }
    }
     */
    "127.0.0.1"
  }
}
