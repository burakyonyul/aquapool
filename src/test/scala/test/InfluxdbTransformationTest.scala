package test

import com.hp.hpl.jena.query.ResultSet
import querying.message.Result

object InfluxdbTransformationTest extends App {

  val influxdbExecutor = new DummyInfluxdbExecutor()
  val query =
    """
      |from(bucket: "mimic-iii")
      ||> range(start:2000-01-01, stop:2012-12-31)
      ||> filter(fn: (r) => r["_measurement"] == "chart_event")
      ||> filter(fn: (r) => r["itemid"] == "678" or r["itemid"] == "198")
      ||> filter(fn: (r) => r["subject_id"] == "191")
      ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
      ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        """.stripMargin

  val result: Result = influxdbExecutor.executeQuery(query).get
  val resultSet: ResultSet = result.toResultSet
  //ResultSetFormatter.out(resultSet)
  while (resultSet.hasNext) {
    val solution = resultSet.next()
    println(s"Solution: [${solution}], Row Number: [${resultSet.getRowNumber}]")
  }
}
