package querying.message

import java.io.ByteArrayInputStream
import com.hp.hpl.jena.query.{ResultSet, ResultSetFactory}
import play.api.libs.json.{JsValue, Json, OFormat}

//By adding `result vars ArrayBuffer[String]` here, the `toResultSet` approach, which is used only to access variables, will be avoided.
case class Result(resultJSON: JsValue, resultVars: Seq[String], key: Int) {
  def toResultSet: ResultSet = {
    val inputStream = new ByteArrayInputStream(resultJSON.toString.getBytes)
    ResultSetFactory.fromJSON(inputStream)
  }
}

object Result {

  implicit val resultFormats: OFormat[Result] = Json.format[Result]

}
