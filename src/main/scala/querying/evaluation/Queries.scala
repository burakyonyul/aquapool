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

  val INFLUXDB_TEMPERATURE_OF_SEPSIS_PATIENT: String =
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "678" or r["itemid"] == "198")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin

  val INFLUXDB_RESPIRATORY_RATE_OF_ACUTE_KIDNEY_INJURY_PATIENT: String =
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2009-10-28T00:00:00Z, stop:2009-12-10T00:00:00Z)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "618")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin

  val REDIS_LRANGE_MAGNESIUM: String = "[0] [lrange] [{1532}]"
  val REDIS_KEY_SEPSIS: String = "[3] [keys] [{*Sepsis*}]"
  val REDIS_LRANGE_SEPSIS_PATIENTS: String = "[6] [reverselrange] [{99591}{99592}]"
  val REDIS_LRANGE_ACUTE_KIDNEY_INJURY_PATIENTS: String = "[6] [reverselrange] [{5849}]"
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

  val POSTGRESQL_DEAD_PATIENTS: String = "SELECT * FROM mimiciii.admissions WHERE admissions.deathtime IS NOT NULL"

  val REDIS_ZRANGE_PROCEDURES_OF_SEPSIS_PATIENT: String = "[5] [zrangeWithScore] [{191-136614}{191-142081}]"

  val POSTGRESQL_ADMISSION_INFO_OF_SEPSIS_PATIENT: String = "SELECT * FROM mimiciii.admissions WHERE admissions.subject_id=191"
  val POSTGRESQL_PROCEDURES_COMBINED_ADMISSION_INFO_OF_SEPSIS_PATIENT: String = "select * from mimiciii.admissions inner join mimiciii.procedures_icd on admissions.subject_id=procedures_icd.subject_id and admissions.hadm_id = procedures_icd.hadm_id where admissions.subject_id=191"
  val POSTGRESQL_PROCEDURES_COMBINED_ADMISSION_INFO_AND_TEMPERATURE_EVENTS_OF_SEPSIS_PATIENT: String =
    s"""
       |select *
       |from mimiciii.admissions
       |         inner join mimiciii.procedures_icd on admissions.subject_id = procedures_icd.subject_id
       |         inner join mimiciii.chartevents on admissions.subject_id = chartevents.subject_id
       |where admissions.subject_id = 191
       |  and chartevents.itemid IN (618, 198)
       |""".stripMargin
}
