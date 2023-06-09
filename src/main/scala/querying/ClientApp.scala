package querying

import akka.actor.ActorSystem
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import com.typesafe.config.ConfigFactory
import querying.actor.Agent
import querying.message.{PolyStoreQuery, Register}

import java.net.{InetAddress, NetworkInterface}
import scala.collection.immutable.HashMap

object ClientApp {

  private val CROSS_DOMAIN_QUERY_3 = "SELECT ?president ?party ?page WHERE { SERVICE <http://localhost:7000/sparql/> {?president <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/President> . ?president <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/United_States> . ?president <http://dbpedia.org/ontology/party> ?party .} SERVICE <http://localhost:9000/sparql/> {?x <http://www.w3.org/2002/07/owl#sameAs> ?president . ?x <http://data.nytimes.com/elements/topicPage> ?page .}}"

  private val LIVE_ENDPOINT_QUERY_TEXT = "PREFIX owl: <http://www.w3.org/2002/07/owl#>" + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" + "PREFIX geo: <http://www.geonames.org/ontology#>" + "PREFIX movie: <http://data.linkedmdb.org/resource/movie/>" + "PREFIX dbo: <http://dbpedia.org/ontology/>" + "PREFIX lgdo: <http://linkedgeodata.org/ontology/>" + "PREFIX yago: <http://dbpedia.org/class/yago/>" + "SELECT ?geoName WHERE {" + "SERVICE <http://www.linkedmdb.org/sparql> " + "{ ?lmdbProducer movie:producer_name \"Steven Spielberg\".}" + "SERVICE <http://dbpedia.org/sparql>" + "{" +
    "?dbpediaProducer dbo:residence <http://dbpedia.org/resource/California>. " + "?dbpediaProducer rdf:type yago:WikicatAmericanFilmProducers." + "?dbpediaProducer rdf:type yago:WikicatAmericanArtCollectors." + "?dbpediaProducer owl:sameAs ?lmdbProducer." + "?dbpediaProducer dbo:birthPlace ?dbpPlace." + "?dbpPlace owl:sameAs ?geoPlace.}" + "SERVICE <http://linkedgeodata.org/sparql>" + "{" + "?geoPlace rdfs:seeAlso <http://dbpedia.org/resource/Cincinnati>." + "?geoPlace geo:name ?geoName.}" + "}"

  private val LIVE_ENDPOINT_QUERY_2_TEXT = "PREFIX owl: <http://www.w3.org/2002/07/owl#>" + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" + "PREFIX movie: <http://data.linkedmdb.org/resource/movie/>" + "PREFIX dbo: <http://dbpedia.org/ontology/>" + "PREFIX lgdo: <http://linkedgeodata.org/ontology/>" + "PREFIX yago: <http://dbpedia.org/class/yago/>" + "SELECT ?geoLabel WHERE {" + "SERVICE <http://dbpedia.org/sparql>" + "{<http://dbpedia.org/resource/Steven_Spielberg> dbo:birthPlace ?dbpPlace." + "?dbpPlace owl:sameAs ?geoPlace.}" + "SERVICE <http://linkedgeodata.org/sparql>" + "{?geoPlace <http://linkedgeodata.org/ontology/is_in%3Acountry> \"USA\"." + "?geoPlace rdf:type lgdo:Place." + "?geoPlace rdfs:label ?geoLabel." + "FILTER(!lang(?geoLabel))" + "}" + "}"

  val GOOD_LOOKING_QUERY =
    """
      | PREFIX owl: <http://www.w3.org/2002/07/owl#>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      | PREFIX geo: <http://www.geonames.org/ontology#>
      | PREFIX movie: <http://data.linkedmdb.org/resource/movie/>
      | PREFIX dbo: <http://dbpedia.org/ontology/>
      | PREFIX lgdo: <http://linkedgeodata.org/ontology/>
      | PREFIX yago: <http://dbpedia.org/class/yago/>
      | SELECT ?geoName WHERE {
      | SERVICE <http://www.linkedmdb.org/sparql>
      | { ?lmdbProducer movie:producer_name "Steven Spielberg".}
      | SERVICE <http://dbpedia.org/sparql>
      | {?dbpediaProducer dbo:residence <http://dbpedia.org/resource/California>.
      | ?dbpediaProducer rdf:type yago:WikicatAmericanFilmProducers.
      | ?dbpediaProducer rdf:type yago:WikicatAmericanArtCollectors.
      | ?dbpediaProducer owl:sameAs ?lmdbProducer.
      | ?dbpediaProducer dbo:birthPlace ?dbpPlace.
      | ?dbpPlace owl:sameAs ?geoPlace.}
      | SERVICE <http://linkedgeodata.org/sparql>
      | {?geoPlace rdfs:seeAlso <http://dbpedia.org/resource/Cincinnati>.
      | ?geoPlace geo:name ?geoName.}}
      |""".stripMargin

  private val GOOD_LOOKING_QUERY_2 =
    """
      | PREFIX owl: <http://www.w3.org/2002/07/owl#>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      | PREFIX geo: <http://www.geonames.org/ontology#>
      | PREFIX movie: <http://data.linkedmdb.org/resource/movie/>
      | PREFIX dbo: <http://dbpedia.org/ontology/>
      | PREFIX lgdo: <http://linkedgeodata.org/ontology/>
      | PREFIX yago: <http://dbpedia.org/class/yago/>
      | SELECT ?dbpPlace WHERE {
      | SERVICE <http://www.linkedmdb.org/sparql>
      | { ?lmdbProducer movie:producer_name "Steven Spielberg".}
      | SERVICE <http://dbpedia.org/sparql>
      | {?dbpediaProducer dbo:residence <http://dbpedia.org/resource/California>.
      | ?dbpediaProducer rdf:type yago:WikicatAmericanFilmProducers.
      | ?dbpediaProducer rdf:type yago:WikicatAmericanArtCollectors.
      | ?dbpediaProducer owl:sameAs ?lmdbProducer.
      | ?dbpediaProducer dbo:birthPlace ?dbpPlace.}}
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    var ipAddress = getIpAddress
    var port = "2553"
    if (args.size > 1) {
      ipAddress = args(0)
      port = args(1)
    }
    val config = ConfigFactory.parseString("akka.remote.artery.canonical.hostname = " + ipAddress).
      withFallback(ConfigFactory.parseString("akka.remote.artery.canonical.port = " + port)).
      withFallback(ConfigFactory.load("agent.conf"))

    // Create an Akka system
    val system = ActorSystem("Subscribing", config)

    val agent = system.actorOf(Agent.props, "Agent")
    val client = system.actorOf(ClusterClient.props(ClusterClientSettings(system)), "client")
    agent ! Register(PolyStoreQuery(HashMap.empty[String, String], ""), client)

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
