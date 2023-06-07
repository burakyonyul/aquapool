package querying.message

import play.api.libs.json.{Json, OFormat}
import querying.message.Store.Store

case class DistributeQuery(query: String, stores: Seq[Store])

object DistributeQuery {

  implicit val dqFormats: OFormat[DistributeQuery] = Json.format[DistributeQuery]

}