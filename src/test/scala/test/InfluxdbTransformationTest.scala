package test

import com.hp.hpl.jena.query.ResultSet
import querying.message.Result

object InfluxdbTransformationTest extends App {

  val influxdbExecutor = new DummyInfluxdbExecutor()
  val query =
    """from(bucket: "mimic-iii")
      |  |> range(start:2010-10-20, stop:2010-10-25)
      |  |> filter(fn: (r) => r["_measurement"] == "chart_event")
      |  |> filter(fn: (r) => r["itemid"] == "1000")
      |  |> filter(fn: (r) => r["subject_id"] == "5183")
      |  |> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "icustay_id" or r["_field"] == "hadm_id")
      |  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        """.stripMargin


  val result: Result = influxdbExecutor.executeQuery(query).get
  val resultSet: ResultSet = result.toResultSet
  while (resultSet.hasNext) {
    println(resultSet.next())
  }
}
