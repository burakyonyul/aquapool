package querying.message

import play.api.libs.json.Json

case class PerformHashJoin(firstRs: Result, secondRs: Result)

object PerformHashJoin {

  implicit val performHashJoinFormats = Json.format[PerformHashJoin]

}
