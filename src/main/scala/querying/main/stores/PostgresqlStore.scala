package querying.main.stores

import java.sql.{DriverManager, ResultSet}

object PostgresqlStore {
  classOf[org.postgresql.Driver]
  val con_st = "jdbc:postgresql://localhost:5432/mimic?user=postgres&password=admin123"
  val conn = DriverManager.getConnection(con_st)
  try {
    val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val rs = stm.executeQuery("SELECT * from mimiciii.admissions LIMIT 5")

    while (rs.next) {
      //println(rs)
      println(rs.getString("row_id") + "--" + rs.getString("hadm_id") + "--" + rs.getTimestamp("admittime"))
    }
  } finally {
    conn.close()
  }

}
