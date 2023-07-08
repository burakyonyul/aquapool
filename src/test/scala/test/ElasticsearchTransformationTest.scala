package test

import com.hp.hpl.jena.query.ResultSetFormatter
import com.sksamuel.elastic4s.ElasticDsl.search
import com.sksamuel.elastic4s.{RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import querying.main.stores.ElasticsearchStore
import querying.message.Result
import querying.transformation.ElasticsearchTransformer
import com.sksamuel.elastic4s.ElasticDsl._

object ElasticsearchTransformationTest extends App {
  var result: Option[Result] = None
  val query =
    s"""
       |{
       |    "bool" : {
       |      "must" : [
       |        { "term" : { "subject_id": "191" } },
       |        { "term" : { "category": "Discharge summary" } },
       |        {
       |            "query_string": {
       |              "query": "lamincetomies",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":30
       |""".stripMargin
  val resp = ElasticsearchStore.client.execute {
    search("noteevents").rawQuery(query)
  }.await

  resp match {
    case failure: RequestFailure => println("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] =>
      result = ElasticsearchTransformer.transformToRdfResult(results)
    case results: RequestSuccess[_] => println(results.result)
  }
  val rdfResultSet = result.get.toResultSet
  ResultSetFormatter.out(rdfResultSet)
  ElasticsearchStore.client.close()
}
