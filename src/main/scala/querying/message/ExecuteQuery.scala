package querying.message

import play.api.libs.json.{Json, OFormat}

case class ExecuteQuery(query: String, queryId: String = "unknown")

object ExecuteQuery {

  implicit val esqFormats: OFormat[ExecuteQuery] = Json.format[ExecuteQuery]

}