package querying.main

import querying.message.Result

import scala.collection.immutable.HashMap

object ResultMap {

  private var map: HashMap[Int, Result] = HashMap.empty

  def insert(result: Result): Unit = {
    map += result.hashCode -> result
  }

  def retrieve(key: Int, result: Result): Unit = {
    if (!map.contains(key)) {
      insert(result)
    }
    map(key)
  }

}
