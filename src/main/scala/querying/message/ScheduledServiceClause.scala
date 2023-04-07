package querying.message

import play.api.libs.json.Json

case class ScheduledServiceClause(executeSubQuery: ExecuteServiceClause)

object ScheduledServiceClause {
  implicit val scheduledFormats = Json.format[ScheduledServiceClause]
}
