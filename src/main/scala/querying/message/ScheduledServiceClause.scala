package querying.message

import play.api.libs.json.Json

case class ScheduledServiceClause(executeSubQuery: ExecuteQuery)

object ScheduledServiceClause {
  implicit val scheduledFormats = Json.format[ScheduledServiceClause]
}
