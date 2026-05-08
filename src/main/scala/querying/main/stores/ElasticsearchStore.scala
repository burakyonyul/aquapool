package querying.main.stores

import com.sksamuel.elastic4s.http.{JavaClient, NoOpRequestConfigCallback}
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback

object ElasticsearchStore {

  val callback = new HttpClientConfigCallback {
    override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
      val creds = new BasicCredentialsProvider()
      creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "admin123"))
      httpClientBuilder.setDefaultCredentialsProvider(creds)
        .setMaxConnTotal(100)      // toplam max bağlantı (default ~10)
        .setMaxConnPerRoute(100)   // route başına max bağlantı
    }
  }


  val props = ElasticProperties("http://155.223.25.1:9200")
  val client = ElasticClient(JavaClient(props, requestConfigCallback = NoOpRequestConfigCallback, httpClientConfigCallback = callback))
  //val props: ElasticProperties = ElasticProperties("http://127.0.0.1:9200")
  //val client: ElasticClient = ElasticClient(JavaClient(props))
}
