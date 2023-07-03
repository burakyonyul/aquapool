package querying

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import querying.actor.Agent
import querying.main.Constants
import querying.message.PolyStoreQuery

import scala.collection.immutable.HashMap

/**
 * TODO: Polystore use cases need to be generated
 */
object AgentApp {

  def main(args: Array[String]): Unit = {
    //val organizationDataList = OrganizationDataReader.readOrganizationData("/organization_data.txt") "/home/burak/Development/monitoring-environment/resources/void"
    //val voidModel = VoidModelConstructor.constructVOIDSpaceModel(System.getProperty("user.dir") + "/src/main/resources/void")

    val ipAddress = if (args.isDefinedAt(0)) args(0) else getIpAddress
    val port = if (args.isDefinedAt(1)) args(1) else "2553"
    val config = ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname = $ipAddress").
      withFallback(ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")).
      withFallback(ConfigFactory.load("agent.conf"))

    // Create an Akka system
    val system = ActorSystem("ClientQuerier", config)

    val agent = system.actorOf(Agent.props, "QuerierClient-1")

    val influxQuery =
      s"""
         |from(bucket: "mimic-iii")
         ||> range(start:2000-01-01, stop:2012-12-31)
         ||> filter(fn: (r) => r["_measurement"] == "chart_event")
         ||> filter(fn: (r) => r["itemid"] == "1532")
         ||> filter(fn: (r) => r["subject_id"] == "21")
         ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
         ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
         |""".stripMargin
    val redisQuery = "[0] [lrange] [{1532}]"
    val queryMap = HashMap(redisQuery -> Constants.REDIS, influxQuery -> Constants.INFLUXDB)
    agent ! PolyStoreQuery(queryMap, "")

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
