package querying.message

object Store extends Enumeration {
  type Store = Value
  val Redis, Postgresql, Influxdb, Elasticsearch = Value
}
