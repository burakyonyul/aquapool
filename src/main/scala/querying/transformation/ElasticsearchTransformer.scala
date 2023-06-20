package querying.transformation

import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.{ModelFactory, ResourceFactory}
import com.sksamuel.elastic4s.RequestSuccess
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import querying.main.{Constants, QueryingUtils}
import querying.message.Result

object ElasticsearchTransformer {

  def transformToRdfResult(results: RequestSuccess[SearchResponse]): Option[Result] = {
    var sparqlBody = ""
    val model = ModelFactory.createDefaultModel()
    val searchHits = results.result.hits.hits.toList
    searchHits.foreach { searchHit =>
      sparqlBody = ""
      val id = searchHit.id
      val noteeventRsc = ResourceFactory.createResource(s"{${Constants.MIMIC_RESOURCE_URI}noteevent/{$id}}")
      val searchMap = searchHit.sourceAsMap
      val keysIterator = searchMap.keysIterator
      while (keysIterator.hasNext) {
        val key = keysIterator.next()
        val keyPrp = ResourceFactory.createProperty(Constants.MIMIC_ONTOLOGY_URI, key)
        model.add(noteeventRsc, keyPrp, searchMap(key).toString)
        sparqlBody +=
          s"""
             |?s mimic-ont:$key ?$key.
             |""".stripMargin
      }
    }
    val sparqlQuery = Constants.GENERIC_SPARQL_PREFIX + sparqlBody + Constants.CLOSE_CURLY_BRACE
    //println(sparqlQuery)
    val result = QueryingUtils.convertRdf2Result(QueryExecutionFactory.create(sparqlQuery, model).execSelect())
    Option(result)
  }

}
