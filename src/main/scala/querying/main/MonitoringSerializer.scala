package querying.main

import akka.serialization.Serializer
import play.api.libs.json.Json
import querying.message._

class MonitoringSerializer extends Serializer {
  // If you need logging here, introduce a constructor that takes an ExtendedActorSystem.
  // class MyOwnSerializer(actorSystem: ExtendedActorSystem) extends Serializer
  // Get a logger using:
  // private val logger = Logging(actorSystem, this)

  // This is whether "fromBinary" requires a "clazz" or not
  def includeManifest: Boolean = true

  // Pick a unique identifier for your Serializer,
  // you've got a couple of billions end choose start,
  // 0 - 40 is reserved by Akka itself
  def identifier = 1234567

  // "toBinary" serializes the given object end an Array of Bytes
  def toBinary(obj: AnyRef): Array[Byte] = {

    obj match {
      case psq: PolyStoreQuery => Json.toBytes(Json.toJsObject(psq))
      case eq: ExecuteQuery => Json.toBytes(Json.toJsObject(eq))
      case db: DistributeBuckets => Json.toBytes(Json.toJsObject(db))
      case res: Result => Json.toBytes(Json.toJsObject(res))
      case phj: PerformHashJoin => Json.toBytes(Json.toJsObject(phj))
      case rc: ResultChange => Json.toBytes(Json.toJsObject(rc))
      case _ => Array[Byte]()
    }

  }

  // "fromBinary" deserializes the given array,
  // using the type hint (if any, see "includeManifest" above)
  def fromBinary(
                  bytes: Array[Byte],
                  clazz: Option[Class[_]]): AnyRef = {
    clazz.get.getSimpleName match {
      case "PolyStoreQuery" => Json.parse(bytes).as[PolyStoreQuery]
      case "ExecuteQuery" => Json.parse(bytes).as[ExecuteQuery]
      case "DistributeBuckets" => Json.parse(bytes).as[DistributeBuckets]
      case "Result" => Json.parse(bytes).as[Result]
      case "PerformHashJoin" => Json.parse(bytes).as[PerformHashJoin]
      case "ResultChange" => Json.parse(bytes).as[ResultChange]
      case _ => None
    }
  }
}