package querying.message

import play.api.libs.json.{Json, OFormat}

case class PerformHashJoin(firstRs: Result, secondRs: Result)

object PerformHashJoin {

  implicit val performHashJoinFormats: OFormat[PerformHashJoin] = Json.format[PerformHashJoin]

}
