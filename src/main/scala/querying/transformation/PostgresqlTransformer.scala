package querying.transformation

import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.{ModelFactory, ResourceFactory}
import querying.main.{Constants, QueryingUtils}
import querying.message.Result

import java.sql.{ResultSet, ResultSetMetaData}

object PostgresqlTransformer {

  def transformToRdfResult(sqlResult: ResultSet): Option[Result] = {
    val metaData = sqlResult.getMetaData
    val tables: Seq[String] = findTables(metaData)
    var columns: Seq[String] = Seq.empty[String]
    val model = ModelFactory.createDefaultModel()
    val resourceDescription: String = generateResourceDescription(tables)

    while (sqlResult.next) {
      val subject = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + resourceDescription + "-" + sqlResult.getRow)
      for (colNo <- 1 to metaData.getColumnCount) {
        var colValue = sqlResult.getString(colNo)
        if (colValue == null) colValue = ""
        val columnName = getColumnName(metaData, columns, colNo)
        columns = columns :+ columnName
        model.add(subject, ResourceFactory.createProperty(Constants.MIMIC_ONTOLOGY_URI, columnName), colValue)
      }
    }

    columns = Seq.empty[String]
    val sparqlPrefix =
      s"""
         |PREFIX mimic-rsc:<${Constants.MIMIC_RESOURCE_URI}>
         |PREFIX mimic-ont:<${Constants.MIMIC_ONTOLOGY_URI}>
         |SELECT * WHERE {
         |""".stripMargin

    var sparqlBody = ""

    for (colNo <- 1 to metaData.getColumnCount) {
      val columnName = getColumnName(metaData, columns, colNo)
      columns = columns :+ columnName
      sparqlBody +=
        s"""
           |?s mimic-ont:$columnName ?$columnName.
           |""".stripMargin
    }

    val sparqlPostfix = "}"

    val sparqlQuery = sparqlPrefix + sparqlBody + sparqlPostfix
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
