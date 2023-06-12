package querying

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import querying.actor.Agent
import querying.main.OrganizationConstants
import querying.message.PolyStoreQuery

import java.net.{InetAddress, NetworkInterface}
import scala.collection.immutable.HashMap

/**
 * TODO: Polystore use cases need end be generated
 */
object AgentApp {

  def main(args: Array[String]): Unit = {
    //val organizationDataList = OrganizationDataReader.readOrganizationData("/organization_data.txt") "/home/burak/Development/monitoring-environment/resources/void"
    //val voidModel = VoidModelConstructor.constructVOIDSpaceModel(System.getProperty("user.dir") + "/src/main/resources/void")

    val ipAddress = if (args.isDefinedAt(0)) args(0) else getIpAddress
    val port = if (args.isDefinedAt(1)) args(1) else "2553"
    val query_count = if (args.isDefinedAt(2)) args(2).toInt else 5000
    val bunch_percent = if (args.isDefinedAt(3)) args(3).toDouble else 0.1D
    val query_selectivity = if (args.isDefinedAt(4)) args(4) else "HIGH"
    val selectionDbpedia = if (args.isDefinedAt(5)) args(5) else "ALL"
    val selectionNytimes = if (args.isDefinedAt(6)) args(6) else "ALL"
    val selectionStock = if (args.isDefinedAt(7)) args(7) else "ALL"
    val config = ConfigFactory.parseString("akka.remote.artery.canonical.hostname = " + ipAddress).
      withFallback(ConfigFactory.parseString("akka.remote.artery.canonical.port = " + port)).
      withFallback(ConfigFactory.load("agent.conf"))

    // Create an Akka system
    val system = ActorSystem("Polystore", config)
    val bunch_count = (query_count * bunch_percent).toInt
    for (index <- 1 to query_count) {
      val agent = system.actorOf(Agent.props, "Agent-" + index)

      val federatedQuery = query_selectivity match {
        case "MOST" => OrganizationConstants.generateMostSelectiveFederatedQuery(index, selectionDbpedia, selectionNytimes, selectionStock)
        case "HIGH" => OrganizationConstants.generateHighSelectiveFederatedQuery(index, selectionDbpedia, selectionNytimes, selectionStock)
        case "MID" => OrganizationConstants.generateMidSelectiveFederatedQuery(index, selectionDbpedia, selectionNytimes, selectionStock)
        case "LOW" => OrganizationConstants.generateLowSelectiveFederatedQuery(index, selectionDbpedia, selectionNytimes, selectionStock)
        case "LEAST" => OrganizationConstants.generateLeastSelectiveFederatedQuery(index)
      }

      //val federatedQuery = String.format(OrganizationConstants.FEDERATED_STOCK_QUERY_TEMPLATE,DBPEDIA_COMPANY_RESOURCE_URI_TEMPLATE+index)
      //TODO: fix the query generation
      agent ! PolyStoreQuery(HashMap.empty[String, String], "")
      if (index % (bunch_count) == 0) {
        //println("Index: "+index + ", bunch count: "+ bunch_count + ", Query count: "+MetricStore.get(Constants.QUERY_COUNT).get + ", Actor count: "+MetricStore.get(Constants.ACTOR_COUNT).get)
        Thread.sleep(60000)
      }
    }


  }

  private def getIpAddress: String = {
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
    return "127.0.0.1"
  }
}
