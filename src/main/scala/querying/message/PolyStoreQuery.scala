package querying.message

import play.api.libs.json.{Json, OFormat}
import querying.message.Store.Store

import scala.collection.immutable.HashMap

case class PolyStoreQuery(queryStoreMap: HashMap[String, Seq[Store]], senderPath: String)

object PolyStoreQuery {
  implicit val psqFormats: OFormat[PolyStoreQuery] = Json.format[PolyStoreQuery]
}

