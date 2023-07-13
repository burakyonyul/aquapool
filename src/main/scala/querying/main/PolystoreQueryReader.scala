package querying.main

import play.api.libs.json.{JsObject, JsValue, Json}
import querying.message.PolyStoreQuery

import scala.collection.immutable.HashMap
import scala.io.Source

object PolystoreQueryReader {

  def read(queryPath: String): PolyStoreQuery = {

    val jsonFileSource = Source.fromFile(queryPath)
    val lines = jsonFileSource.getLines
    val source: String = lines.mkString
    jsonFileSource.close()
    val psQueryValue: JsValue = Json.parse(source)
    val psQueryObject: JsObject = psQueryValue.as[JsObject]

    var psQueryMap = HashMap.empty[String, String]
    for ((store: String, query: JsValue) <- psQueryObject.fields) {
      store match {
        case Constants.REDIS =>
          val redisQuery = query.as[String]
          psQueryMap += redisQuery -> Constants.REDIS
        case Constants.ELASTICSEARCH =>
          var elasticQuery = Json.stringify(query)
          if (elasticQuery.length > 1) {
            psQueryMap += elasticQuery.concat(
              s"""
                 |,"size": 10000
                 |""".stripMargin) -> Constants.ELASTICSEARCH
          } else {
            println("Invalid elasticsearch query")
          }
        case Constants.POSTGRESQL =>
          val postgresQuery = query.as[String]
          psQueryMap += postgresQuery -> Constants.POSTGRESQL
        case Constants.INFLUXDB =>
          val influxQuery = query.as[String]
          psQueryMap += influxQuery -> Constants.INFLUXDB
      }

    }
    PolyStoreQuery(psQueryMap, "")
  }
}
