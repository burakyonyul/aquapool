package querying.message

import play.api.libs.json.Json

case class FederateQuery(query: String, senderPath: String)

object FederateQuery {

  implicit val fqFormats = Json.format[FederateQuery]

}