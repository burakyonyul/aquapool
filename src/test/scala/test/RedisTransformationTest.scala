package test

import com.hp.hpl.jena.query.ResultSet
import querying.main.stores.RedisStore
import querying.transformation.RedisTransformer

object RedisTransformationTest extends App {


  //queryRedisAsRDF(0, "6", "110", "lrange")
  //queryRedisAsRDF(1, "50817", "50823", "lrange")
  //queryRedisAsRDF(2, "016", "0139", "lrange")
  //queryRedisAsRDF(3, "01105", "0479", "lrange")
  //queryRedisAsRDF(4, "14021", "14839", "lrange")
  //queryRedisAsRDF(5, "108-123552", "530-149648", "zrangeWithScore")
  //queryRedisAsRDF(6, "570-100913", "1430-184067", "zrangeWithScore")

  private def queryRedisAsRDF(database: Int, firstKey: String, secondKey: String, command: String) = {
    var firstValues: Option[Any] = None
    var secondValues: Option[Any] = None
    command match {
      case "lrange" =>
        firstValues = RedisStore.lrange(database, firstKey, 0, 501)
        secondValues = RedisStore.lrange(database, secondKey, 0, 501)
      case "zrangeWithScore" =>
        firstValues = RedisStore.zrangeWithScore(database, firstKey)
        secondValues = RedisStore.zrangeWithScore(database, secondKey)
    }

    val rs: ResultSet = RedisTransformer.transformToRdfResult(database, Map(firstKey -> firstValues, secondKey -> secondValues)).get.toResultSet
    while (rs.hasNext) {
      val solution = rs.next()
      println(solution)
    }
  }


}
