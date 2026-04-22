package querying.evaluation

import querying.main.Constants

import scala.collection.immutable.HashMap

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
  val REDIS_LRANGE_SEPSIS_PATIENTS: String = "[6] [reverselrange] [{99591}]"
  val REDIS_LRANGE_SEPSIS_SEVERESEPSIS_PATIENTS: String = "[6] [reverselrange] [{99591}{99592}]"
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

  // Time Series - SQL Queries
  //---------------------------------------------------------------------------------------
  val Query_A_1_1: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE itemid=220050 AND (charttime BETWEEN '2200-01-01 00:00:00' AND '2209-12-31 23:59:00')
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_2: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE subject_id=191 AND itemid=211
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_3: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE subject_id=191 AND itemid=618
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_4: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE subject_id=191 AND itemid=646
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_5: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE subject_id=191 AND itemid=678
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_6: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE subject_id=191 AND itemid=198
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_7: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE chartevents.subject_id=191 AND (chartevents.itemid=211 OR chartevents.itemid=618)
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_8: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE chartevents.subject_id=191 AND (chartevents.itemid=211 OR chartevents.itemid=618 OR chartevents.itemid=646)
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_9: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE chartevents.subject_id=191 AND (chartevents.itemid=211 OR chartevents.itemid=618 OR chartevents.itemid=646 OR chartevents.itemid=678)
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_10: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE chartevents.subject_id=191 AND (chartevents.itemid=211 OR chartevents.itemid=618 OR chartevents.itemid=646 OR chartevents.itemid=678 OR chartevents.itemid=198)
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_11: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.chartevents WHERE chartevents.subject_id=191
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_12: Map[String, String] = HashMap(
    s"""
       |SELECT chartevents.* FROM mimiciii.chartevents WHERE chartevents.subject_id=191 AND (charttime BETWEEN '2196-04-10 08:00:00' AND '2196-04-10 12:00:00')
       |""".stripMargin -> Constants.POSTGRESQL)
  val Query_A_1_13: Map[String, String] = HashMap(
    s"""
       |SELECT chartevents.* FROM mimiciii.chartevents WHERE chartevents.subject_id=191 AND (charttime BETWEEN '2196-04-10 11:00:00' AND '2196-04-10 15:00:00')
       |""".stripMargin -> Constants.POSTGRESQL)
  //---------------------------------------------------------------------------------------

  // Time Series - Flux Queries
  //---------------------------------------------------------------------------------------
  val Query_A_2_1: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2011-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "220050")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id" or r["_tag"] == "subject_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_2: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "211")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_3: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "618")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_4: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "646")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_5: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "678")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_6: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "198")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_7: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "211" or r["itemid"] == "618")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_8: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "211" or r["itemid"] == "618" or r["itemid"] == "646")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_9: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "211" or r["itemid"] == "618" or r["itemid"] == "646" or r["itemid"] == "678")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_10: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "211" or r["itemid"] == "618" or r["itemid"] == "646" or r["itemid"] == "678" or r["itemid"] == "198")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_11: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id" or r["_tag"] == "itemid")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_12: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2010-04-10T08:00:00Z, stop:2010-04-10T12:00:00Z)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_2_13: Map[String, String] = HashMap(
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2010-04-10T11:00:00Z, stop:2010-04-10T15:00:00Z)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  //---------------------------------------------------------------------------------------

  // Key-Value - SQL Queries
  //---------------------------------------------------------------------------------------

  val Query_A_3_1: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE itemid = 211
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_2: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE itemid = 618
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_3: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE itemid = 646
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_4: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE itemid = 678
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_5: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE itemid = 198
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_6: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE itemid IN (211, 618, 646, 678, 198)
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_7: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.procedures_icd WHERE subject_id=191 AND (hadm_id=142081 OR hadm_id=136614)
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_8: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.diagnoses_icd WHERE subject_id=191 AND (hadm_id=142081 OR hadm_id=136614)
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_9: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE label LIKE 'Magnesium'
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_3_10: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.d_items WHERE label LIKE 'Arterial Blood Pressure%'
       |""".stripMargin -> Constants.POSTGRESQL)

  //---------------------------------------------------------------------------------------


  // Key-Value - Redis Queries
  //---------------------------------------------------------------------------------------
  val Query_A_4_1: Map[String, String] = HashMap(
    s"""
       |[0] [lrange] [{211}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_2: Map[String, String] = HashMap(
    s"""
       |[0] [lrange] [{618}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_3: Map[String, String] = HashMap(
    s"""
       |[0] [lrange] [{646}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_4: Map[String, String] = HashMap(
    s"""
       |[0] [lrange] [{678}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_5: Map[String, String] = HashMap(
    s"""
       |[0] [lrange] [{198}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_6: Map[String, String] = HashMap(
    s"""
       |[0] [lrange] [{211} {618} {646} {678} {198}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_7: Map[String, String] = HashMap(
    s"""
       |[5] [zrangeWithScore] [{191-142081}{191-136614}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_8: Map[String, String] = HashMap(
    s"""
       |[6] [zrangeWithScore] [{191-142081}{191-136614}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_9: Map[String, String] = HashMap(
    s"""
       |[0] [keys] [{Magnesium}]
       |""".stripMargin -> Constants.REDIS)
  val Query_A_4_10: Map[String, String] = HashMap(
    s"""
       |[0] [keys] [{Arterial Blood Pressure*}]
       |""".stripMargin -> Constants.REDIS)
  //---------------------------------------------------------------------------------------

  // Document-Search - SQL Queries
  //---------------------------------------------------------------------------------------
  val Query_A_5_1: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents WHERE LOWER(text) LIKE '%hypersplenism%'
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_5_2: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents WHERE LOWER(text) LIKE '%glomerulonephritis%'
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_5_3: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents WHERE category='Discharge summary' AND LOWER(text) LIKE '%glomerulonephritis%'
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_5_4: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents WHERE subject_id=191 AND LOWER(text) LIKE '%blood pressure%'
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_5_5: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents WHERE subject_id=191 AND category='Discharge summary' AND LOWER(text) LIKE '%blood pressure%'
       |""".stripMargin -> Constants.POSTGRESQL)
  //---------------------------------------------------------------------------------------


  // Document-Search ** Elasticsearch Queries
  //---------------------------------------------------------------------------------------
  val Query_A_6_1: Map[String, String] = HashMap(
    s"""
       |{
       |    "bool" : {
       |      "must" : [
       |        {
       |            "query_string": {
       |              "query": "Hypersplenism",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":300
       |""".stripMargin -> Constants.ELASTICSEARCH)

  val Query_A_6_2: Map[String, String] = HashMap(
    s"""
       |{
       |    "bool" : {
       |      "must" : [
       |        {
       |            "query_string": {
       |              "query": "glomerulonephritis",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":1000
       |""".stripMargin -> Constants.ELASTICSEARCH)

  val Query_A_6_3: Map[String, String] = HashMap(
    s"""
       |{
       |"bool" : {
       |      "must" : [
       |   { "term" : { "category": "Discharge summary" } },
       |        {
       |            "query_string": {
       |              "query": "glomerulonephritis",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":1000
       |""".stripMargin -> Constants.ELASTICSEARCH)

  val Query_A_6_4: Map[String, String] = HashMap(
    s"""
       |{
       |"bool" : {
       |      "must" : [
       |	   { "term" : { "subject_id": "191" } },
       |        {
       |            "query_string": {
       |              "query": "blood pressure",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":1000
       |""".stripMargin -> Constants.ELASTICSEARCH)

  val Query_A_6_5: Map[String, String] = HashMap(
    s"""
       |{
       |"bool" : {
       |      "must" : [
       |	   { "term" : { "subject_id": "191" } },
       |   	   { "term" : { "category": "Discharge summary" } },
       |        {
       |            "query_string": {
       |              "query": "blood pressure",
       |              "default_field": "text"
       |            }
       |        }
       |      ]
       |    }
       |},
       |"size":1000
       |""".stripMargin -> Constants.ELASTICSEARCH)

  //---------------------------------------------------------------------------------------


  // Join - SQL Queries
  //---------------------------------------------------------------------------------------
  val Query_A_7_1: Map[String, String] = HashMap(
    s"""
       |SELECT chartevents.*,d_items.label FROM
       |mimiciii.chartevents INNER JOIN mimiciii.d_items
       |ON chartevents.itemid = d_items.itemid
       |WHERE chartevents.itemid=1532 AND chartevents.subject_id=21
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_7_2: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents
       |INNER JOIN mimiciii.diagnoses_icd ON noteevents.subject_id=diagnoses_icd.subject_id
       |WHERE LOWER(noteevents.text) LIKE '%azotemia%' AND diagnoses_icd.icd9_code='99591'
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_7_3: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents
       |INNER JOIN mimiciii.diagnoses_icd ON noteevents.subject_id=diagnoses_icd.subject_id
       |WHERE LOWER(noteevents.text) LIKE '%azotemia%' AND
       |(diagnoses_icd.icd9_code IN('99591','99592'))
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_7_4: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents
       |INNER JOIN mimiciii.diagnoses_icd
       |ON noteevents.subject_id = diagnoses_icd.subject_id
       |INNER JOIN mimiciii.admissions
       |ON noteevents.hadm_id = admissions.hadm_id
       |WHERE LOWER(noteevents.text) LIKE '%azotemia%'
       |AND (diagnoses_icd.icd9_code IN ('99591', '99592'))
       |AND admissions.deathtime IS NOT NULL
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_7_5: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.admissions INNER JOIN mimiciii.procedures_icd ON
       |admissions.subject_id=procedures_icd.subject_id AND
       |admissions.hadm_id = procedures_icd.hadm_id WHERE admissions.subject_id=191
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_7_6: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.admissions
       |INNER JOIN mimiciii.procedures_icd ON admissions.subject_id = procedures_icd.subject_id
       |INNER JOIN mimiciii.chartevents ON admissions.subject_id = chartevents.subject_id
       |WHERE admissions.subject_id = 191
       |AND chartevents.itemid IN (678)
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_7_7: Map[String, String] = HashMap(
    s"""
       |SELECT * FROM mimiciii.noteevents
       |INNER JOIN mimiciii.diagnoses_icd ON noteevents.subject_id = diagnoses_icd.subject_id
       |INNER JOIN mimiciii.admissions ON noteevents.hadm_id = admissions.hadm_id
       |INNER JOIN mimiciii.chartevents ON admissions.subject_id = chartevents.subject_id
       |WHERE LOWER(noteevents.text) LIKE '%azotemia%'
       |AND (diagnoses_icd.icd9_code = '5849')
       |AND admissions.deathtime IS NOT NULL
       |AND chartevents.itemid = 618
       |AND chartevents.charttime BETWEEN '2180-10-01' AND '2189-12-31';
       |""".stripMargin -> Constants.POSTGRESQL)
  //---------------------------------------------------------------------------------------

  // Polystore Queries
  //---------------------------------------------------------------------------------------
  val Query_A_8_1: Map[String, String] = HashMap(
    s"""
       |[0] [lrange] [{1532}]
       |""".stripMargin -> Constants.REDIS,
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "1532")
       ||> filter(fn: (r) => r["subject_id"] == "21")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_8_2: Map[String, String] = HashMap(
    s"""
       |[6]	[reverselrange]	[{99591}]
       |""".stripMargin -> Constants.REDIS,
    s"""
       |{
       |"bool" : {
       |        "must" : [
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
       |""".stripMargin -> Constants.ELASTICSEARCH)

  val Query_A_8_3: Map[String, String] = HashMap(
    s"""
       |[6]	[reverselrange]	[{99591}{99592}]
       |""".stripMargin -> Constants.REDIS,
    s"""
       |{
       |"bool" : {
       |        "must" : [
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
       |""".stripMargin -> Constants.ELASTICSEARCH)

  val Query_A_8_4: Map[String, String] = HashMap(
    s"""
       |[6]	[reverselrange]	[{99591}{99592}]
       |""".stripMargin -> Constants.REDIS,
    s"""
       |SELECT * FROM mimiciii.admissions WHERE admissions.deathtime IS NOT NULL
       |""".stripMargin -> Constants.POSTGRESQL,
    s"""
       |{
       |"bool" : {
       |        "must" : [
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
       |""".stripMargin -> Constants.ELASTICSEARCH)

  val Query_A_8_5: Map[String, String] = HashMap(
    s"""
       |[5] [zrangeWithScore] [{191-136614}{191-142081}]
       |""".stripMargin -> Constants.REDIS,
    s"""
       |SELECT * FROM mimiciii.admissions WHERE subject_id=191
       |""".stripMargin -> Constants.POSTGRESQL)

  val Query_A_8_6: Map[String, String] = HashMap(
    s"""
       |[5] [zrangeWithScore] [{191-136614}{191-142081}]
       |""".stripMargin -> Constants.REDIS,
    s"""
       |SELECT * FROM mimiciii.admissions WHERE subject_id=191
       |""".stripMargin -> Constants.POSTGRESQL,
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2000-01-01, stop:2012-12-31)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "678")
       ||> filter(fn: (r) => r["subject_id"] == "191")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  val Query_A_8_7: Map[String, String] = HashMap(
    s"""
       |[6] [reverselrange] [{5849}]
       |""".stripMargin -> Constants.REDIS,
    s"""
       |SELECT * FROM mimiciii.admissions WHERE admissions.deathtime IS NOT NULL
       |""".stripMargin -> Constants.POSTGRESQL,
    s"""
       |{
       |"bool" : {
       |        "must" : [
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
       |""".stripMargin -> Constants.ELASTICSEARCH,
    s"""
       |from(bucket: "mimic-iii")
       ||> range(start:2009-10-28T00:00:00Z, stop:2009-12-10T00:00:00Z)
       ||> filter(fn: (r) => r["_measurement"] == "chart_event")
       ||> filter(fn: (r) => r["itemid"] == "618")
       ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
       ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
       |""".stripMargin -> Constants.INFLUXDB)

  //---------------------------------------------------------------------------------------
}
