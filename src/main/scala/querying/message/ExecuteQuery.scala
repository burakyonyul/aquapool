package querying.message

import play.api.libs.json.Json
import querying.message.Store.Store

case class ExecuteQuery(query: String, store: Store)

object ExecuteQuery {

  implicit val esqFormats = Json.format[ExecuteQuery]

}