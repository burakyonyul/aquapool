package querying.message

import play.api.libs.json.Json

case class DistributeServiceClause(query: String, endpoints: Seq[String])

object DistributeServiceClause {

  implicit val fsqFormats = Json.format[DistributeServiceClause]

}