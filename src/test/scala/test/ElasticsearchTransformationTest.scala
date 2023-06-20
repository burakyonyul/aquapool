package test

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
       |  "query": {
       |    "bool" : {
       |      "must" : [
       |        { "match" : { "hadm_id": "163230" } },
       |        { "match" : { "subject_id": "69047" },
       |        { "match" : { "category": "Nursing" } ,
       |        { "match" : { "cgid": "20816" } ,
       |        { "match" : { "charttime": "2192-01-12 15:03:00" }  }
       |      ]
       |    }
       |  }
       |}
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
  println(result)
}
