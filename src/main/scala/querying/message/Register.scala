package querying.message

import akka.actor.ActorRef
import play.api.libs.json.{Json, OFormat}

case class Register(psq: PolyStoreQuery, client: ActorRef)

object Register {
  implicit val registerFormats: OFormat[Register] = Json.format[Register]
}
