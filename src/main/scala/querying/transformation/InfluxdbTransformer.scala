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
  private var sparqlQuery = ""

  def generateMeasurementResource(fluxRecord: FluxRecord): Unit = {
    val fieldValues = fluxRecord.getValues.keySet()
    //println(fieldValues)
    val fieldIter = fieldValues.iterator()
    val measurementName = fluxRecord.getMeasurement
    val measurementRsc = ResourceFactory.createResource(s"${Constants.MIMIC_RESOURCE_URI}$measurementName/${UUID.randomUUID()}")
    while (fieldIter.hasNext) {
      val fieldKey = fieldIter.next()
      if (fieldKey != "row_id") {
        val fieldPrp = ResourceFactory.createProperty(Constants.MIMIC_ONTOLOGY_URI, fieldKey)
        //println(fluxRecord.getValueByKey(fieldKey))
        val value = if (fluxRecord.getValueByKey(fieldKey) != null) fluxRecord.getValueByKey(fieldKey).toString else ""
        model.add(measurementRsc, fieldPrp, value)
        if (sparqlQuery.isEmpty) {
          sparqlBody +=
            s"""
               |?influxRsc mimic-ont:$fieldKey ?$fieldKey.
               |""".stripMargin
        }
      }
    }
    sparqlQuery = Constants.GENERIC_SPARQL_PREFIX + sparqlBody + Constants.CLOSE_CURLY_BRACE
  }

  def transformToRdfResult(): Option[Result] = {
    println(sparqlQuery)
    val result = QueryingUtils.convertRdf2Result(QueryExecutionFactory.create(sparqlQuery, model).execSelect())
    Option(result)
  }

}
