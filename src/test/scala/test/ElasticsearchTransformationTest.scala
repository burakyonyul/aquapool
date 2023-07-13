package test

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import com.sksamuel.elastic4s.{RequestFailure, RequestSuccess}
import querying.main.stores.ElasticsearchStore
import querying.message.Result
import querying.transformation.ElasticsearchTransformer

object ElasticsearchTransformationTest extends App {
  var result: Option[Result] = None
  val query =
    s"""
       |{
       |    "bool" : {
       |      "must" : [
       |        {
       |            "query_string": {
       |              "query": "azotemia",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":10000
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
  //ResultSetFormatter.out(rdfResultSet)
  while (rdfResultSet.hasNext) {
    val solution = rdfResultSet.next()
    println(s"Solution: [${solution}], Row Number: [${rdfResultSet.getRowNumber}]")
  }
  ElasticsearchStore.client.close()
}
