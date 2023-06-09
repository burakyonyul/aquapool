package querying.message

import play.api.libs.json.{Json, OFormat}

case class PolyStoreQuery(queryStoreMap: Map[String, String], senderPath: String)

object PolyStoreQuery {
  implicit val psqFormats: OFormat[PolyStoreQuery] = Json.format[PolyStoreQuery]
}

