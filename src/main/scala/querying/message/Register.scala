package querying.message

import akka.actor.ActorRef

case class Register(query: String, client: ActorRef)

object Register {

}
