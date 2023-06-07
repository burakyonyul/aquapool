package querying.message

import play.api.libs.json.{Json, OFormat}

case class DistributeBuckets(first: Result, second: Result)

object DistributeBuckets {

  implicit val splitFormats: OFormat[DistributeBuckets] = Json.format[DistributeBuckets]

}
