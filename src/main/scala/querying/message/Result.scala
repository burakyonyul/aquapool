package querying.message

import java.io.ByteArrayInputStream
import com.hp.hpl.jena.query.{ResultSet, ResultSetFactory}
import play.api.libs.json.{JsValue, Json, OFormat}

//result vars ArrayBuffer[String] olarak buraya da eklenerek yalnızca variable'lara erişmek için yapılan toResultSet durumundan kaçınılmış olacak
case class Result(resultJSON: JsValue, resultVars: Seq[String], key: Int) {
  def toResultSet: ResultSet = {
    val inputStream = new ByteArrayInputStream(resultJSON.toString.getBytes)
    ResultSetFactory.fromJSON(inputStream)
  }
}

object Result {

  implicit val resultFormats: OFormat[Result] = Json.format[Result]

}
