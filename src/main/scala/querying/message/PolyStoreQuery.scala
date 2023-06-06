package querying.message

import play.api.libs.json.Json
import querying.message.Store.Store

import scala.collection.immutable.HashMap

case class PolyStoreQuery(queryStoreMap: HashMap[String, Seq[Store]], senderPath: String)

object PolyStoreQuery {
  implicit val psqFormats = Json.format[PolyStoreQuery]
}

