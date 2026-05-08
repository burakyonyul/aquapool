package querying.main.stores

import com.influxdb.client.scala.{InfluxDBClientScala, InfluxDBClientScalaFactory}

object InfluxdbStore {
  // local token
  //val token = "_g0QMff1wsKFHft1sf6rFs4VwKgFYINv7DGHiGlRGzlI5-Kd70rUEk4swdohDJYlf29OMbvv4grDyV-uR8sqSw=="
  // remote token
  //val old_token = "Kc5sQAW14uasogduCq6WibiPGuFNVQyjGJMcVbzGYSCv8WMH01xkhhn6D4MQKTjMbHTXQFpnWaP9Fb0IWGcnDg=="
  val token = "ZDrMqNqlXAoKbCuJMc-FE1HehdtPZSjh2a2ZKV20Wu9DFxNuiPIMkrdxCbWHcfnc-KESPZ2SQE8mFIAzRQ2Ulg=="
  val org = "EgeUniversity"
  val bucket = "mimic-iii"
  // local server URL
  //val serverURL = "http://localhost:8086?readTimeout=999999"
  // remote server URL
  val serverURL = "http://155.223.25.2:8086?readTimeout=999999"

  //val client: InfluxDBClientScala = InfluxDBClientScalaFactory.create(serverURL, token.toCharArray, org, bucket)

  def getClient(): InfluxDBClientScala = {
    InfluxDBClientScalaFactory.create(serverURL, token.toCharArray, org, bucket)
  }
}