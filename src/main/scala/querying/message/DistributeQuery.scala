package querying.message

import play.api.libs.json.Json
import querying.message.Store.Store

case class DistributeQuery(query: String, stores: Seq[Store])

object DistributeQuery {

  implicit val fsqFormats = Json.format[DistributeQuery]

}