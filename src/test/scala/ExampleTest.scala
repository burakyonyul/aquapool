import org.scalatest.FlatSpec
import play.api.libs.json.Json
import querying.main.ResultMap
import querying.message.Result

class ExampleTest extends FlatSpec {
  "A ResultMap" should "store results as single objects and serve them with no duplicates" in {
    val result1 = new Result(Json.parse("{'x':'y'}"), Vector.empty[String], 1)
    val result2 = new Result(Json.parse("{'a':'b'}"), Vector.empty[String], 1)
    ResultMap.insert(result1)
    val res1 = ResultMap.retrieve(result1.hashCode, result1)

    val res2_1 = ResultMap.retrieve(result2.hashCode, result2)
    val res2_2 = ResultMap.retrieve(result2.hashCode, result2)
  }
}
