package querying.message

import play.api.libs.json.Json

case class ResultChange(result: Result, detectionTime: Long)

object ResultChange {

  implicit val changeFormats = Json.format[ResultChange]

}
