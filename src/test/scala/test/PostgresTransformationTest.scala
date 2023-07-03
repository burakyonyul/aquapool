package test

import querying.main.stores.PostgresqlStore
import querying.transformation.PostgresqlTransformer

import java.sql.{Connection, ResultSet}

object PostgresTransformationTest extends App {

  private val conn: Connection = PostgresqlStore.hikariDataSource.getConnection
  //queryPostgresqlAsRDF("SELECT * FROM mimiciii.patients LIMIT 10")
  queryPostgresqlAsRDF(
    s"""
       |select * from mimiciii.chartevents where  itemid=1532 and subject_id=21
       |""".stripMargin)

  private def queryPostgresqlAsRDF(sqlQuery: String) = {
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs: ResultSet = stm.executeQuery(sqlQuery)
      val result = PostgresqlTransformer.transformToRdfResult(rs)
      val rdfRS = result.get.toResultSet
      //ResultSetFormatter.out(rdfRS)
      while (rdfRS.hasNext) {
        val solution = rdfRS.next()
        println(solution)
      }
    } finally {
      conn.close()
    }
  }

}
