package querying.transformation

import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.{ModelFactory, ResourceFactory}
import querying.main.{Constants, QueryingUtils}
import querying.message.Result

import java.sql.{ResultSet, ResultSetMetaData}

object PostgresqlTransformer {

  /**
   * Need to fix this
   */
  def transformToRdfResult(sqlResult: ResultSet): Option[Result] = {
    val metaData = sqlResult.getMetaData
    val tables: Seq[String] = findTables(metaData)
    var columns: Seq[String] = Seq.empty[String]
    val model = ModelFactory.createDefaultModel()
    val resourceDescription: String = generateResourceDescription(tables)

    while (sqlResult.next) {
      columns = Seq.empty[String]
      val subject = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + resourceDescription + "-" + sqlResult.getRow)
      for (colNo <- 1 to metaData.getColumnCount) {
        if (metaData.getColumnName(colNo) != "row_id") {
          var colValue = sqlResult.getString(colNo)
          if (colValue == null) colValue = ""
          val columnName = getColumnName(metaData, columns, colNo)
          columns = columns :+ columnName
          model.add(subject, ResourceFactory.createProperty(Constants.MIMIC_ONTOLOGY_URI, columnName), colValue)
        }
      }
    }
    //model.write(System.out)
    columns = Seq.empty[String]

    var sparqlBody = ""

    for (colNo <- 1 to metaData.getColumnCount) {
      val columnName = getColumnName(metaData, columns, colNo)
      if (columnName != "row_id") {
        columns = columns :+ columnName
        sparqlBody +=
          s"""
             |?postgresRsc mimic-ont:$columnName ?$columnName.
             |""".stripMargin
      }
    }


    val sparqlQuery = Constants.GENERIC_SPARQL_PREFIX + sparqlBody + Constants.CLOSE_CURLY_BRACE
    //println(QueryFactory.create(sparqlQuery))
    //println(sparqlQuery)
    val result = QueryingUtils.convertRdf2Result(QueryExecutionFactory.create(sparqlQuery, model).execSelect())
    Option(result)
  }

  private def getColumnName(metaData: ResultSetMetaData, columns: Seq[String], colNo: Int): String = {
    var columnName = metaData.getColumnName(colNo)
    if (columns.contains(columnName)) {
      columnName = metaData.getTableName(colNo) + "_" + columnName
    }
    columnName
  }

  private def generateResourceDescription(tables: Seq[String]) = {
    var resourceDescription = ""
    for (table <- tables) {
      resourceDescription += table + "-"
    }
    resourceDescription.substring(0, resourceDescription.length - 1)
  }

  private def findTables(metaData: ResultSetMetaData) = {
    var tables = Seq.empty[String]
    for (colNo <- 1 to metaData.getColumnCount) {
      val table = metaData.getTableName(colNo)
      if (!tables.contains(table)) {
        tables = tables :+ table
      }
    }
    tables
  }
}
