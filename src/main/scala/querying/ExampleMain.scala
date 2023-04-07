package querying

import com.hp.hpl.jena.query.{QueryExecutionFactory, QueryFactory}
import querying.main.{MonitoringUtils, OrganizationConstants}
import org.apache.spark.util.SizeEstimator

object ExampleMain {
  def main(args: Array[String]): Unit = {
    /*println(QueryFactory.create(OrganizationConstants.generateFederatedQueryWithMultipleSelection(1)))
    println(QueryFactory.create(OrganizationConstants.generateGenericFederatedQuery(1,"ALL","ALL","ALL")))
    println(QueryFactory.create(OrganizationConstants.generateGenericFederatedQuery(1,"2000","2000","2000")))
    println(QueryFactory.create(OrganizationConstants.generateFederatedQueryForSpecificDbpediaCompany(6001,"2000","2000", "2000")))
    println(QueryFactory.create(OrganizationConstants.generateFederatedQueryForSpecificDbpediaCompany(5001,"ALL","ALL","ALL")))
*/
    println(QueryFactory.create(OrganizationConstants.generateHighSelectiveFederatedQuery(5001,"MIN","MIN","MIN")))
    println(QueryFactory.create(OrganizationConstants.generateHighSelectiveFederatedQuery(5001,"300","300","300")))
    println(QueryFactory.create(OrganizationConstants.generateMostSelectiveFederatedQuery(5001,"MIN","MIN","MIN")))
    println(QueryFactory.create(OrganizationConstants.generateMostSelectiveFederatedQuery(5001,"200","200","200")))
    /*val queryExecution = QueryExecutionFactory.create(OrganizationConstants.generateHighlySelectiveFederatedQuery(1), ModelFactory.createDefaultModel())
    val resultSet = queryExecution.execSelect()
    ResultSetFormatter.out(resultSet)
     */

    val query_count=20001
    val bunch_percent=10
    val bunch_count = query_count * bunch_percent / 100
    println(bunch_count)
    println(("0.01667".toDouble*60000).toInt)
    println(0.5D)

    println((query_count-1)%2000)

    println((0.04445*22500).toInt)

    //println(String.format(OrganizationConstants.FEDERATED_STOCK_QUERY_TEMPLATE,"http://dbpedia.org/resource/company-1"))
    //val query = QueryFactory.create(OrganizationConstants.generateFederatedQueryWithMultipleSelection(1))
    //print(query)
    //val resultSet = QueryExecutionFactory.create(query, ModelFactory.createDefaultModel()).execSelect()
    //ResultSetFormatter.outputAsJSON(resultSet)
    /*val wodqaEngine = new WodqaEngine()
    val resultSet = wodqaEngine.executeSelect(OrganizationConstants.generateFederatedQueryWithMultipleSelection(1))
    while(resultSet.hasNext){
      val solution = resultSet.next()
      println(solution)
    }*/

    val resultSet = QueryExecutionFactory.sparqlService("http://155.223.25.4:8890/dbpedia/sparql", "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\nPREFIX dbo: <http://dbpedia.org/ontology/>\nPREFIX dbpedia: <http://dbpedia.org/resource/>\nPREFIX nytimes: <http://data.nytimes.com/elements/>\nPREFIX stockmarket: <http://stockmarket.com/elements/>\nPREFIX owl: <http://www.w3.org/2002/07/owl#> Select * where {\n  <http://dbpedia.org/resource/company-282> dbo:numberOfStaff ?staffCount282.\n\t<http://dbpedia.org/resource/company-282> owl:sameAs ?nytCompany282.}").execSelect()
    val result = MonitoringUtils.convertRdf2Result(resultSet)
    println("Size: "+SizeEstimator.estimate(result))

    val resultSet2 = QueryExecutionFactory.sparqlService("http://155.223.25.1:8890/nytimes/sparql", "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\nPREFIX dbo: <http://dbpedia.org/ontology/>\nPREFIX dbpedia: <http://dbpedia.org/resource/>\nPREFIX nytimes: <http://data.nytimes.com/elements/>\nPREFIX stockmarket: <http://stockmarket.com/elements/>\nPREFIX owl: <http://www.w3.org/2002/07/owl#> Select * where {\t?nytCompany282 rdf:type nytimes:Company.\n\t?nytCompany282 nytimes:reputation 'Elite'^^xsd:string.\n  ?nytCompany282 nytimes:associated_article_count ?articleCount282.}").execSelect()
    val result2 = MonitoringUtils.convertRdf2Result(resultSet2)
    println("Size: "+SizeEstimator.estimate(result2))

    val resultSet3 = QueryExecutionFactory.sparqlService("http://155.223.25.1:8890/nytimes/sparql", "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\nPREFIX dbo: <http://dbpedia.org/ontology/>\nPREFIX dbpedia: <http://dbpedia.org/resource/>\nPREFIX nytimes: <http://data.nytimes.com/elements/>\nPREFIX stockmarket: <http://stockmarket.com/elements/>\nPREFIX owl: <http://www.w3.org/2002/07/owl#> Select * where {\t ?nytCompany28 rdf:type nytimes:Company.\n\t?nytCompany28 nytimes:reputation ?reputation28.\n  ?nytCompany28 nytimes:associated_article_count ?articleCount28.}").execSelect()
    val result3 = MonitoringUtils.convertRdf2Result(resultSet3)
    println("Size: "+SizeEstimator.estimate(result3))

    val query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\nPREFIX dbo: <http://dbpedia.org/ontology/>\nPREFIX dbpedia: <http://dbpedia.org/resource/>\nPREFIX nytimes: <http://data.nytimes.com/elements/>\nPREFIX stockmarket: <http://stockmarket.com/elements/>\nPREFIX owl: <http://www.w3.org/2002/07/owl#> Select * where {\t ?nytCompany28 rdf:type stockmarket:Company.\n\t?nytCompany28 stockmarket:market ?market28.\n\t?nytCompany28 stockmarket:currency ?currency28.\n \t?nytCompany28 stockmarket:stockPrice ?stockPrice28.}"
    val resultSet4 = QueryExecutionFactory.sparqlService("http://155.223.25.2:8890/stockmarket/sparql", query).execSelect()
    val result4 = MonitoringUtils.convertRdf2Result(resultSet4)
    println("Size: "+(SizeEstimator.estimate(result4).toDouble/(1024*1024))+" MB")
    println("Size of Query: "+SizeEstimator.estimate(query))

  }
}
