package querying.evaluation

object Queries {
  val INFLUXDB_ITEM_OF_SUBJECT: String =
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "1532")
       ||> filter(fn: (r) => r["subject_id"] == "21")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin
  val REDIS_LRANGE_MAGNESIUM: String = "[0] [lrange] [{1532}]"
  val REDIS_KEY_SEPSIS: String = "[3] [keys] [{*Sepsis*}]"
  val REDIS_LRANGE_SEPSIS_PATIENTS: String = "[6] [reverselrange] [{99591}{99592}]"
  val ELASTICSEARCH_AZOTEMIA_PATIENTS: String =
    s"""
       |{
       |    "bool" : {
       |      "must" : [
       |        {
       |            "query_string": {
       |              "query": "azotemia",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":10000
       |""".stripMargin
}
