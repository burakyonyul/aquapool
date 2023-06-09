package querying.message

import akka.actor.ActorRef

case class Register(psq: PolyStoreQuery, client: ActorRef)

object Register {
}
