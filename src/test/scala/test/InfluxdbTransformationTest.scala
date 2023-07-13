package test

import com.hp.hpl.jena.query.ResultSet
import querying.message.Result

object InfluxdbTransformationTest extends App {

  val influxdbExecutor = new DummyInfluxdbExecutor()
  val query =
    """
      |from(bucket: "mimic-iii")
      ||> range(start:2006-08-04T00:00:00Z, stop:2006-11-05T00:00:00Z)
      ||> filter(fn: (r) => r["_measurement"] == "chart_event")
      ||> filter(fn: (r) => r["subject_id"] == "29035")
      ||> filter(fn: (r) => r["itemid"] == "618")
      ||> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or (r["_field"] == "hadm_id") or r["_field"] == "deidentifiedcharttime")
      ||> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        """.stripMargin

  val result: Result = influxdbExecutor.executeQuery(query).get
  val resultSet: ResultSet = result.toResultSet
  //ResultSetFormatter.out(resultSet)
  while (resultSet.hasNext) {
    val solution = resultSet.next()
    if (solution.getLiteral("hadm_id").getString == "154213") {
      println(s"Solution: [${solution}], Row Number: [${resultSet.getRowNumber}]")
    }
  }
}
