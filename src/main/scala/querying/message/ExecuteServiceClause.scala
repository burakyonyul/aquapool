package querying.message

import play.api.libs.json.Json

case class ExecuteServiceClause(query: String, endpoint: String)

object ExecuteServiceClause {

  implicit val esqFormats = Json.format[ExecuteServiceClause]

}