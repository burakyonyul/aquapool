package querying.transformation

import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import com.influxdb.query.FluxRecord
import querying.main.{Constants, QueryingUtils}
import querying.message.Result

import java.util.UUID

class InfluxdbTransformer {

  private val model: Model = ModelFactory.createDefaultModel()
  private var sparqlBody = ""

  def generateMeasurementResource(fluxRecord: FluxRecord): Unit = {
    val fieldValues = fluxRecord.getValues.keySet()
    val fieldIter = fieldValues.iterator()
    val measurementName = fluxRecord.getMeasurement
    val measurementRsc = ResourceFactory.createResource(s"${Constants.MIMIC_RESOURCE_URI}$measurementName/${UUID.randomUUID()}")
    while (fieldIter.hasNext) {
      val fieldKey = fieldIter.next()
      val fieldPrp = ResourceFactory.createProperty(Constants.MIMIC_ONTOLOGY_URI, fieldKey)
      model.add(measurementRsc, fieldPrp, fluxRecord.getValueByKey(fieldKey).toString)
      sparqlBody +=
        s"""
           |?s mimic-ont:$fieldKey ?$fieldKey.
           |""".stripMargin
    }
  }

  def transformToRdfResult(): Option[Result] = {
    val sparqlQuery = Constants.GENERIC_SPARQL_PREFIX + sparqlBody + Constants.CLOSE_CURLY_BRACE
    val result = QueryingUtils.convertRdf2Result(QueryExecutionFactory.create(sparqlQuery, model).execSelect())
    Option(result)
  }

}
