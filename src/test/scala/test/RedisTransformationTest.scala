package test

import com.hp.hpl.jena.query.ResultSet
import querying.main.stores.RedisStore
import querying.transformation.RedisTransformer

object RedisTransformationTest extends App {


  //regexMatchCommands()

  private def regexMatchCommands() = {
    val text = "[database] [operation_name] [{key1}, {key2}, {key3}, ...]"
    val regex1 = "(?<=\\[)(.*?)(?=\\])".r
    val matchList: List[String] = regex1.findAllIn(text).toList
    val firstMatch: String = matchList.head
    val secondMatch: String = matchList(1)
    val lastMatch: String = matchList.last
    println(firstMatch)
    println(secondMatch)
    println(lastMatch)

    val regex2 = "(?<=\\{)(.*?)(?=\\})".r
    val subMatchList: List[String] = regex2.findAllIn(lastMatch).toList
    for (subMatch <- subMatchList) {
      println(subMatch)
    }
  }

  //queryRedisAsRDF(0, "Arterial Blood Pressure*", "Magnesium", "keys")
  //queryRedisAsRDF(1, "*Glucose*", "*Cholesterol*", "keys")
  //queryRedisAsRDF(2, "*Blood vessel*", "*alcohol and drug*", "keys")
  //queryRedisAsRDF(3, "*Cleft lip,*", "*sleep disorders*", "keys")
  //queryRedisAsRDF(0, "6", "110", "lrange")
  //queryRedisAsRDF(1, "50817", "50823", "lrange")
  //queryRedisAsRDF(2, "016", "0139", "lrange")
  //queryRedisAsRDF(3, "01105", "0479", "lrange")
  //queryRedisAsRDF(4, "14021", "14839", "lrange")
  //queryRedisAsRDF(5, "108-123552", "530-149648", "zrangeWithScore")
  //queryRedisAsRDF(6, "570-100913", "1430-184067", "zrangeWithScore")
  //queryRedisAsRDF(4, "RN", "UA", "reverselrange")
  //queryRedisAsRDF(5, "9671", "8872", "reverselrange")
  //queryRedisAsRDF(6, "V3000", "7793", "reverselrange")
  queryRedisAsRDF(6, "5849", "", "reverselrange")

  private def queryRedisAsRDF(database: Int, firstKey: String, secondKey: String, command: String) = {
    var firstValues: Option[Any] = None
    var secondValues: Option[Any] = None
    command match {
      case "reverselrange" =>
        firstValues = RedisStore.lrange(database, firstKey, 0, -1)
      //secondValues = RedisStore.lrange(database, secondKey, 0, -1)
      case "keys" =>
        firstValues = RedisStore.keys(database, s"""$firstKey""")
      //secondValues = RedisStore.keys(database, s"""$secondKey""")
      case "lrange" =>
        firstValues = RedisStore.lrange(database, firstKey, 0, -1)
      //secondValues = RedisStore.lrange(database, secondKey, 0, -1)
      case "zrangeWithScore" =>
        firstValues = RedisStore.zrangeWithScore(database, firstKey)
      //secondValues = RedisStore.zrangeWithScore(database, secondKey)
    }

    val rs: ResultSet = RedisTransformer.transformToRdfResult(database, command, Map(firstKey -> firstValues)).get.toResultSet
    while (rs.hasNext) {
      val solution = rs.next()
      if (solution.getLiteral("subject_id").getString == "29035") {
        println(s"Solution: [$solution] - Row Number: [${rs.getRowNumber}]")
      }
    }
  }


}
