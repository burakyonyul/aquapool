package querying.transformation

import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.{ModelFactory, ResourceFactory}
import querying.main.{Constants, QueryingUtils}
import querying.message.Result

import java.sql.{ResultSet, ResultSetMetaData}

object PostgresqlTransformer {

  def transformToRdfResult(sqlResult: ResultSet): Result = {
    val metaData = sqlResult.getMetaData
    val tables: Seq[String] = findTables(metaData)
    val model = ModelFactory.createDefaultModel()
    val resourceDescription: String = generateResourceDescription(tables)
    val subject = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + resourceDescription)

    val sparqlPrefix =
      s"""
         |PREFIX mimic-rsc:<${Constants.MIMIC_RESOURCE_URI}>
         |PREFIX mimic-ont:<${Constants.MIMIC_ONTOLOGY_URI}>
         |SELECT * WHERE {
         |""".stripMargin
    val sparqlPostfix = "}"
    var sparqlBody = ""
    while (sqlResult.next) {
      for (colNo <- 1 to metaData.getColumnCount) {
        model.add(subject, ResourceFactory.createProperty(Constants.MIMIC_ONTOLOGY_URI, metaData.getColumnName(colNo)), sqlResult.getString(colNo))
        sparqlBody +=
          s"""
             |?s mimic-ont:${metaData.getColumnName(colNo)} ?${metaData.getColumnName(colNo)}.
             |""".stripMargin
      }
    }
    val sparqlQuery = sparqlPrefix + sparqlBody + sparqlPostfix
    QueryingUtils.convertRdf2Result(QueryExecutionFactory.create(sparqlQuery, model).execSelect())
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
        tables += table
      }
    }
    tables
  }
}
