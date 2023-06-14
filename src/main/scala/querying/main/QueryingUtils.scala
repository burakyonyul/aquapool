package querying.main

import com.hp.hpl.jena.query.{ResultSet, ResultSetFormatter}
import play.api.libs.json.Json
import querying.message.Result

import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._

object QueryingUtils {
  def convertRdf2Result(rdfResult: ResultSet) = {
    val json = convertRdf2Json(rdfResult)
    Result(json, rdfResult.getResultVars.asScala, 1)
  }

  def convertRdf2Json(rdfResult: ResultSet) = {
    val outputStream = new ByteArrayOutputStream
    ResultSetFormatter.outputAsJSON(outputStream, rdfResult)
    Json.parse(outputStream.toByteArray)
  }

  def formatByteValue(sizeInBytes: Long): String = {
    val sizeInText =
      if (sizeInBytes >= Constants.Kilobytes && sizeInBytes < Constants.Megabytes) {
        sizeInBytes.toDouble / Constants.Kilobytes + " KB"
      }
      else if (sizeInBytes >= Constants.Megabytes && sizeInBytes < Constants.Gigabytes) {
        sizeInBytes.toDouble / Constants.Megabytes + " MB"
      }
      else if (sizeInBytes >= Constants.Gigabytes) {
        sizeInBytes.toDouble / Constants.Gigabytes + " GB"
      }
      else {
        sizeInBytes + " B"
      }
    sizeInText
  }
}
