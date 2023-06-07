package querying.message

import play.api.libs.json.{Json, OFormat}

case class ResultChange(result: Result, detectionTime: Long)

object ResultChange {

  implicit val changeFormats: OFormat[ResultChange] = Json.format[ResultChange]

}
