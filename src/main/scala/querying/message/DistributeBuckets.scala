package querying.message

import play.api.libs.json.Json

case class DistributeBuckets(first: Result, second: Result)

object DistributeBuckets {

  implicit val splitFormats = Json.format[DistributeBuckets]

}
