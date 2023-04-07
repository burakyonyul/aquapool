package querying.main

object OrganizationConstants {
  /**
    * TODO: Sorgu federated yapılacak
    */
  val STOCK_QUERY_TEMPLATE: String = "SELECT ?nytCompany ?articleCount ?stockPrice WHERE {" +
    "<%s> <http://www.w3.org/2002/07/owl#sameAs> ?nytCompany. " +
    "?nytCompany <http://data.nytimes.com/elements/associated_article_count> ?articleCount." +
    "?nytCompany <http://stockmarket.com/elements/stockPrice> ?stockPrice}"
  val FEDERATED_STOCK_QUERY_TEMPLATE: String = "SELECT  ?nytCompany ?articleCount ?stockPrice WHERE {" +
    "SERVICE <http://155.223.25.4:8890/dbpedia/sparql> {" +
    "<%s> <http://www.w3.org/2002/07/owl#sameAs> ?nytCompany." +
    "}SERVICE <http://155.223.25.1:8890/nytimes/sparql> {" +
    "?nytCompany <http://data.nytimes.com/elements/associated_article_count> ?articleCount." +
    "}SERVICE <http://155.223.25.2:8890/stockmarket/sparql> {" +
    "?nytCompany <http://stockmarket.com/elements/stockPrice> ?stockPrice. }}"

  val FEDERATED_TYPED_STOCK_QUERY_TEMPLATE: String = "SELECT  ?nytCompany ?articleCount ?stockPrice WHERE {" +
    "SERVICE <http://155.223.25.4:8890/dbpedia/sparql> {" +
    "<%s> <http://www.w3.org/2002/07/owl#sameAs> ?nytCompany." +
    "}SERVICE <http://155.223.25.1:8890/nytimes/sparql> {" +
    "?nytCompany <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.nytimes.com/elements/Company>." +
    "?nytCompany <http://data.nytimes.com/elements/associated_article_count> ?articleCount." +
    "}SERVICE <http://155.223.25.2:8890/stockmarket/sparql> {" +
    "?nytCompany <http://stockmarket.com/elements/stockPrice> ?stockPrice. }}"

  val FEDERATED_FILTERED_TYPED_STOCK_QUERY_TEMPLATE: String = "SELECT  ?nytCompany ?articleCount ?stockPrice WHERE {" +
    "SERVICE <http://155.223.25.4:8890/dbpedia/sparql> {" +
    "<%s> <http://www.w3.org/2002/07/owl#sameAs> ?nytCompany." +
    "}SERVICE <http://155.223.25.1:8890/nytimes/sparql> {" +
    "?nytCompany <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.nytimes.com/elements/Company>." +
    "?nytCompany <http://data.nytimes.com/elements/associated_article_count> ?articleCount." +
    "FILTER (?articleCount > 40)" +
    "}SERVICE <http://155.223.25.2:8890/stockmarket/sparql> {" +
    "?nytCompany <http://stockmarket.com/elements/stockPrice> ?stockPrice. }}"

  val RESOURCE_PATH = "src/main/resources/"
  val VOID_PATH: String = RESOURCE_PATH + "void/"
  val OWL_SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
  private val DBPEDIA_COMPANY_RESOURCE_URI_TEMPLATE = "http://dbpedia.org/resource/company-"

  def createStockQuery(companyURI: String, index: Int) = {
    s"""SELECT ?nytCompany$index ?articleCount$index ?stockPrice$index WHERE {
       |<$companyURI> <http://www.w3.org/2002/07/owl#sameAs> ?nytCompany$index.
       |?nytCompany$index <http://data.nytimes.com/elements/associated_article_count> ?articleCount$index.
       |?nytCompany$index <http://stockmarket.com/elements/stockPrice> ?stockPrice$index}""".stripMargin
  }

  def createFederatedGenericDbpediaQuery(index: Int) = {
    s"""SERVICE <http://155.223.25.4:8890/dbpedia/sparql> {"
      "|?dbpediaCompany$index <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Company>."
      "|?dbpediaCompany$index <http://www.w3.org/2002/07/owl#sameAs> ?nytCompany$index."
      "|}SERVICE <http://155.223.25.1:8890/nytimes/sparql> {"
      "|?nytCompany$index <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.nytimes.com/elements/Company>."
      "|?nytCompany$index <http://data.nytimes.com/elements/associated_article_count> ?articleCount$index."
      "|}SERVICE <http://155.223.25.2:8890/stockmarket/sparql> {"
      "|?nytCompany$index <http://stockmarket.com/elements/stockPrice> ?stockPrice$index. }}""".stripMargin
  }

  def generateLowSelectiveFederatedQuery(index: Int, selectionDBP: String, selectionNYT: String, selectionSTK: String): String = {

    var (lowerBound, upperBound, reputationValues, marketValues) = (0, 999999, "", "")
    var filterDBpedia = ""
    var valuesNytimes = ""
    var valuesStockmarket = ""

    selectionDBP match {
      case "ALL" => filterDBpedia = ""
      case "4500" => lowerBound = 10000; upperBound = 100000;
      case "4000" => lowerBound = 20000; upperBound = 100000;
      case "3500" => lowerBound = 30000; upperBound = 100000;
      case "3000" => lowerBound = 40000; upperBound = 100000;
      case "2500" => lowerBound = 50000; upperBound = 100000;
      case "2000" => lowerBound = 60000; upperBound = 100000;
      case "1500" => lowerBound = 70000; upperBound = 100000;
      case "1000" => lowerBound = 80000; upperBound = 100000;
      case "500" => lowerBound = 90000; upperBound = 100000;
      case _ => filterDBpedia = ""
    }

    selectionNYT match {
      case "ALL" => valuesNytimes = ""
      case "4000" => reputationValues = "('Very High'^^xsd:string)('High'^^xsd:string)('Medium'^^xsd:string)('Low'^^xsd:string)";
      case "3000" => reputationValues = "('Very High'^^xsd:string)('High'^^xsd:string)('Medium'^^xsd:string)";
      case "2000" => reputationValues = "('Very High'^^xsd:string)('High'^^xsd:string)";
      case "1000" => reputationValues = "('Very High'^^xsd:string)";
      case _ => valuesNytimes = ""
    }

    selectionSTK match {
      case "ALL" => valuesStockmarket = ""
      case "4000" => marketValues = "('NYSE'^^xsd:string)('TSE'^^xsd:string)('FWB'^^xsd:string)('LSE'^^xsd:string)";
      case "3000" => marketValues = "('NYSE'^^xsd:string)('TSE'^^xsd:string)('FWB'^^xsd:string)";
      case "2000" => marketValues = "('NYSE'^^xsd:string)('TSE'^^xsd:string)";
      case "1000" => marketValues = "('NYSE'^^xsd:string)";
      case _ => valuesStockmarket = ""
    }

    if (lowerBound > 0 || upperBound < 999999) filterDBpedia = s"FILTER (?staffCount$index>$lowerBound&&?staffCount$index<=$upperBound)"
    if (reputationValues.nonEmpty) valuesNytimes = s"VALUES (?reputation$index) {$reputationValues}"
    if (marketValues.nonEmpty) valuesStockmarket = s"VALUES (?market$index) {$marketValues}"

    val templateQuery =
      s"""
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |PREFIX dbo: <http://dbpedia.org/ontology/>
         |PREFIX dbpedia: <http://dbpedia.org/resource/>
         |PREFIX nytimes: <http://data.nytimes.com/elements/>
         |PREFIX stockmarket: <http://stockmarket.com/elements/>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |Select * where {
         |service <http://155.223.25.4:8890/dbpedia/sparql> {
         |	?dbpediaCompany$index rdf:type dbo:Company.
         |	?dbpediaCompany$index dbo:numberOfStaff ?staffCount$index.
         |	?dbpediaCompany$index owl:sameAs ?nytCompany$index. $filterDBpedia}
         |service <http://155.223.25.1:8890/nytimes/sparql> {
         |	$valuesNytimes ?nytCompany$index rdf:type nytimes:Company.
         |	?nytCompany$index nytimes:reputation ?reputation$index.
         |  ?nytCompany$index nytimes:associated_article_count ?articleCount$index.}
         |service <http://155.223.25.2:8890/stockmarket/sparql> {
         |	$valuesStockmarket ?nytCompany$index rdf:type stockmarket:Company.
         |	?nytCompany$index stockmarket:market ?market$index.
         |	?nytCompany$index stockmarket:currency ?currency$index.
         | 	?nytCompany$index stockmarket:stockPrice ?stockPrice$index.}
         |}
         |""".stripMargin

    templateQuery
  }

  def generateHighSelectiveFederatedQuery(index: Int, selectionDBP: String, selectionNYT: String, selectionSTK: String): String = {

    var (lowerBound, upperBound, industryValues) = (0, 500, "")
    var filterStock = ""
    var valuesDbpedia = ""
    var filterDbpedia = s"FILTER (?staffCount$index>90000&&?staffCount$index<=100000)"

    selectionDBP match {
      case "ALL" => valuesDbpedia = ""
      case "400" => industryValues = "(dbpedia:Bank)(dbpedia:Airline)(dbpedia:Software)(dbpedia:Electronics)";
      case "300" => industryValues = "(dbpedia:Bank)(dbpedia:Airline)(dbpedia:Software)";
      case "200" => industryValues = "(dbpedia:Bank)(dbpedia:Airline)";
      case "100" => industryValues = "(dbpedia:Bank)";
      case "MIN" => industryValues = "(dbpedia:Restaurant)"; filterDbpedia=""
      case _ => valuesDbpedia = ""
    }

    val reputationValueNYT = selectionNYT match {
      case "ALL" => "'Elite'^^xsd:string"
      case "MIN" => "'Initial'^^xsd:string"
      case _ => "'Elite'^^xsd:string"
    }

    selectionSTK match {
      case "ALL" => lowerBound = 20000; upperBound = 70000;
      case "400" => lowerBound = 30000; upperBound = 70000;
      case "300" => lowerBound = 40000; upperBound = 70000;
      case "200" => lowerBound = 50000; upperBound = 70000;
      case "100" => lowerBound = 60000; upperBound = 70000;
      case "MIN" => lowerBound = 1000; upperBound = 5001;
      case _ => lowerBound = 20000; upperBound = 70000;
    }

    filterStock = s"FILTER (?valueChange$index>=$lowerBound&&?valueChange$index<$upperBound)"
    if (industryValues.nonEmpty) valuesDbpedia = s"VALUES (?industry$index) {$industryValues}"

    val templateQuery =
      s"""
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |PREFIX dbo: <http://dbpedia.org/ontology/>
         |PREFIX dbpedia: <http://dbpedia.org/resource/>
         |PREFIX nytimes: <http://data.nytimes.com/elements/>
         |PREFIX stockmarket: <http://stockmarket.com/elements/>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |Select * where {
         |service <http://155.223.25.4:8890/dbpedia/sparql> {
         |	$valuesDbpedia
         |  ?dbpediaCompany$index rdf:type dbo:Company.
         |	?dbpediaCompany$index dbo:numberOfStaff ?staffCount$index.
         |  ?dbpediaCompany$index dbo:industry ?industry$index.
         |	?dbpediaCompany$index owl:sameAs ?nytCompany$index. $filterDbpedia}
         |service <http://155.223.25.1:8890/nytimes/sparql> {
         |	?nytCompany$index rdf:type nytimes:Company.
         |	?nytCompany$index nytimes:reputation $reputationValueNYT.
         |  ?nytCompany$index nytimes:associated_article_count ?articleCount$index.}
         |service <http://155.223.25.2:8890/stockmarket/sparql> {
         |	?nytCompany$index rdf:type stockmarket:Company.
         |	?nytCompany$index stockmarket:market ?market$index.
         |	?nytCompany$index stockmarket:currency ?currency$index.
         | 	?nytCompany$index stockmarket:stockPrice ?stockPrice$index.
         |  ?nytCompany$index stockmarket:valueChange ?valueChange$index. $filterStock}
         |}
         |""".stripMargin

    templateQuery
  }


  def generateMostSelectiveFederatedQuery(index: Int, selectionDBP: String, selectionNYT: String, selectionSTK: String): String = {

    var (lowerBound, upperBound, industryValues) = (0, 500, "")
    var filterStock = ""
    var valuesDbpedia = ""


    val generatedIndex = selectionDBP match {
      case "ALL" => ((index - 1) % 500) + 1
      case "400" => ((index - 1) % 400) + 1
      case "300" => ((index - 1) % 300) + 1
      case "200" => ((index - 1) % 200) + 1
      case "100" => ((index - 1) % 100) + 1
      case "MIN" => ((index - 1) % 100) + 4901
      case _ => ((index - 1) % 500) + 1
    }

    val dbpediaCompanyURI = DBPEDIA_COMPANY_RESOURCE_URI_TEMPLATE + generatedIndex

    val reputationValueNYT = selectionNYT match {
      case "ALL" => "'Elite'^^xsd:string"
      case "MIN" => "'Initial'^^xsd:string"
      case _ => "'Elite'^^xsd:string"
    }

    selectionSTK match {
      case "ALL" => lowerBound = 20000; upperBound = 70000;
      case "400" => lowerBound = 30000; upperBound = 70000;
      case "300" => lowerBound = 40000; upperBound = 70000;
      case "200" => lowerBound = 50000; upperBound = 70000;
      case "100" => lowerBound = 60000; upperBound = 70000;
      case "MIN" => lowerBound = 1000; upperBound = 5001;
      case _ => lowerBound = 20000; upperBound = 70000;
    }

    filterStock = s"FILTER (?valueChange$index>=$lowerBound&&?valueChange$index<$upperBound)"
    if (industryValues.nonEmpty) valuesDbpedia = s"VALUES (?industry$index) {$industryValues}"

    val templateQuery =
      s"""
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |PREFIX dbo: <http://dbpedia.org/ontology/>
         |PREFIX dbpedia: <http://dbpedia.org/resource/>
         |PREFIX nytimes: <http://data.nytimes.com/elements/>
         |PREFIX stockmarket: <http://stockmarket.com/elements/>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |Select * where {
         |service <http://155.223.25.4:8890/dbpedia/sparql> {
         |  <$dbpediaCompanyURI> dbo:numberOfStaff ?staffCount$index.
         |	<$dbpediaCompanyURI> owl:sameAs ?nytCompany$index.}
         |service <http://155.223.25.1:8890/nytimes/sparql> {
         |	?nytCompany$index rdf:type nytimes:Company.
         |	?nytCompany$index nytimes:reputation $reputationValueNYT.
         |  ?nytCompany$index nytimes:associated_article_count ?articleCount$index.}
         |service <http://155.223.25.2:8890/stockmarket/sparql> {
         |	?nytCompany$index rdf:type stockmarket:Company.
         |	?nytCompany$index stockmarket:market ?market$index.
         |	?nytCompany$index stockmarket:currency ?currency$index.
         | 	?nytCompany$index stockmarket:stockPrice ?stockPrice$index.
         |  ?nytCompany$index stockmarket:valueChange ?valueChange$index. $filterStock}
         |}
         |""".stripMargin

    templateQuery
  }


  def generateMidSelectiveFederatedQuery(index: Int, selectionDBP: String, selectionNYT: String, selectionSTK: String): String = {
    var (reputationValues, marketValues) = ("", "")
    var valuesNytimes = ""
    var valuesStockmarket = ""
    val generatedIndex = selectionDBP match {
      case "ALL" => ((index - 1) % 5000) + 1
      case "4500" => ((index - 1) % 4500) + 1
      case "4000" => ((index - 1) % 4000) + 1
      case "3500" => ((index - 1) % 3500) + 1
      case "3000" => ((index - 1) % 3000) + 1
      case "2500" => ((index - 1) % 2500) + 1
      case "2000" => ((index - 1) % 2000) + 1
      case "1500" => ((index - 1) % 1500) + 1
      case "1000" => ((index - 1) % 1000) + 1
      case "500" => ((index - 1) % 500) + 1
      case _ => ((index - 1) % 5000) + 1
    }
    val dbpediaCompanyURI = DBPEDIA_COMPANY_RESOURCE_URI_TEMPLATE + generatedIndex
    selectionNYT match {
      case "ALL" => valuesNytimes = ""
      case "4000" => reputationValues = "('Very High'^^xsd:string)('High'^^xsd:string)('Medium'^^xsd:string)('Low'^^xsd:string)";
      case "3000" => reputationValues = "('Very High'^^xsd:string)('High'^^xsd:string)('Medium'^^xsd:string)";
      case "2000" => reputationValues = "('Very High'^^xsd:string)('High'^^xsd:string)";
      case "1000" => reputationValues = "('Very High'^^xsd:string)";
      case _ => valuesNytimes = ""
    }

    selectionSTK match {
      case "ALL" => valuesStockmarket = ""
      case "4000" => marketValues = "('NYSE'^^xsd:string)('TSE'^^xsd:string)('FWB'^^xsd:string)('LSE'^^xsd:string)";
      case "3000" => marketValues = "('NYSE'^^xsd:string)('TSE'^^xsd:string)('FWB'^^xsd:string)";
      case "2000" => marketValues = "('NYSE'^^xsd:string)('TSE'^^xsd:string)";
      case "1000" => marketValues = "('NYSE'^^xsd:string)";
      case _ => valuesStockmarket = ""
    }

    if (reputationValues.nonEmpty) valuesNytimes = s"VALUES (?reputation$index) {$reputationValues}"
    if (marketValues.nonEmpty) valuesStockmarket = s"VALUES (?market$index) {$marketValues}"

    val templateQuery =
      s"""
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |PREFIX dbo: <http://dbpedia.org/ontology/>
         |PREFIX dbpedia: <http://dbpedia.org/resource/>
         |PREFIX nytimes: <http://data.nytimes.com/elements/>
         |PREFIX stockmarket: <http://stockmarket.com/elements/>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |Select * where {
         |service <http://155.223.25.4:8890/dbpedia/sparql> {
         |	<$dbpediaCompanyURI> dbo:numberOfStaff ?staffCount$index.
         |	<$dbpediaCompanyURI> owl:sameAs ?nytCompany$index.}
         |service <http://155.223.25.1:8890/nytimes/sparql> {
         |	$valuesNytimes ?nytCompany$index rdf:type nytimes:Company.
         |	?nytCompany$index nytimes:reputation ?reputation$index.
         |  ?nytCompany$index nytimes:associated_article_count ?articleCount$index.}
         |service <http://155.223.25.2:8890/stockmarket/sparql> {
         |	$valuesStockmarket ?nytCompany$index rdf:type stockmarket:Company.
         |	?nytCompany$index stockmarket:market ?market$index.
         |	?nytCompany$index stockmarket:currency ?currency$index.
         | 	?nytCompany$index stockmarket:stockPrice ?stockPrice$index.}
         |}
         |""".stripMargin

    templateQuery
  }

  def generateLeastSelectiveFederatedQuery(index: Int): String = {
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |PREFIX dbo: <http://dbpedia.org/ontology/>
       |PREFIX dbpedia: <http://dbpedia.org/resource/>
       |PREFIX nytimes: <http://data.nytimes.com/elements/>
       |PREFIX stockmarket: <http://stockmarket.com/elements/>
       |PREFIX owl: <http://www.w3.org/2002/07/owl#>
       |Select * where { {BIND(<http://155.223.25.4:8890/dbpedia/sparql> AS ?ser$index)} UNION {BIND(<http://155.223.25.1:8890/nytimes/sparql> AS ?ser$index)} SERVICE ?ser$index {
       |	VALUES (?type$index) {(dbo:Company)(nytimes:Company)(stockmarket:Company)} ?company$index rdf:type ?type$index.
       |  ?company$index rdfs:label ?label$index.
       |	?company$index owl:sameAs ?sameCompany$index.
       |}
       |service <http://155.223.25.2:8890/stockmarket/sparql>{
       |	?company$index stockmarket:market ?market$index.
       |	?company$index stockmarket:currency ?currency$index.
       |  ?company$index stockmarket:stockPrice ?stockPrice$index.
       |}
       |}
       |""".stripMargin
  }

  def generateHighlySelectiveFederatedQuery(index: Int): String = {
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |PREFIX dbo: <http://dbpedia.org/ontology/>
       |PREFIX dbpedia: <http://dbpedia.org/resource/>
       |PREFIX nytimes: <http://data.nytimes.com/elements/>
       |PREFIX stockmarket: <http://stockmarket.com/elements/>
       |PREFIX owl: <http://www.w3.org/2002/07/owl#>
       |Select * where {
       |service <http://155.223.25.4:8890/dbpedia/sparql> {
       |  ?company$index rdf:type dbo:Company.
       |	?company$index dbo:numberOfStaff ?staffCount$index.
       |	?company$index owl:sameAs ?nytCompany$index.  FILTER (?staffCount$index>100 && ?staffCount$index<=1000)}
       |service <http://155.223.25.1:8890/nytimes/sparql> {
       |  ?nytCompany$index rdf:type nytimes:Company.
       |  ?nytCompany$index nytimes:reputation "Elite"^^xsd:string.
       |  ?nytCompany$index nytimes:associated_article_count ?articleCount$index.
       |}
       |service <http://155.223.25.2:8890/stockmarket/sparql>{
       |	?nytCompany$index rdf:type stockmarket:Company.
       |  ?nytCompany$index stockmarket:valueChange ?valueChange$index. FILTER (?valueChange$index>=10000&&?valueChange$index<=15000)
       |  ?nytCompany$index stockmarket:stockPrice ?stockPrice$index.
       |}
       |}
       |""".stripMargin
  }

  def generateSuperSelectiveFederatedQuery(index: Int, dbpediaCompanyURI: String): String = {
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |PREFIX dbo: <http://dbpedia.org/ontology/>
       |PREFIX dbpedia: <http://dbpedia.org/resource/>
       |PREFIX nytimes: <http://data.nytimes.com/elements/>
       |PREFIX stockmarket: <http://stockmarket.com/elements/>
       |PREFIX owl: <http://www.w3.org/2002/07/owl#>
       |Select * where {
       |service <http://155.223.25.4:8890/dbpedia/sparql> {
       |	<$dbpediaCompanyURI> dbo:numberOfStaff ?staffCount$index.
       |	<$dbpediaCompanyURI> owl:sameAs ?nytCompany$index.}
       |service <http://155.223.25.1:8890/nytimes/sparql> {
       |  ?nytCompany$index rdf:type nytimes:Company.
       |  ?nytCompany$index nytimes:reputation "Elite"^^xsd:string.
       |  ?nytCompany$index nytimes:associated_article_count ?articleCount$index.
       |}
       |service <http://155.223.25.2:8890/stockmarket/sparql>{
       |	?nytCompany$index rdf:type stockmarket:Company.
       |  ?nytCompany$index stockmarket:valueChange ?valueChange$index. FILTER (?valueChange$index>=10000&&?valueChange$index<=15000)
       |  ?nytCompany$index stockmarket:stockPrice ?stockPrice$index.
       |}
       |}
       |""".stripMargin
  }


  val GENERIC_FEDERATED_QUERY_WITH_MULTIPLE_SELECTION =
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |PREFIX dbo: <http://dbpedia.org/ontology/>
       |PREFIX dbpedia: <http://dbpedia.org/resource/>
       |PREFIX nytimes: <http://data.nytimes.com/elements/>
       |PREFIX stockmarket: <http://stockmarket.com/elements/>
       |PREFIX owl: <http://www.w3.org/2002/07/owl#>
       |Select * where { {BIND(<http://155.223.25.4:8890/dbpedia/sparql> AS ?ser)} UNION {BIND(<http://155.223.25.1:8890/nytimes/sparql> AS ?ser)} SERVICE ?ser {
       |	?company rdf:type ?type. VALUES (?type) {(dbo:Company)(nytimes:Company)(stockmarket:Company)}
       |	?company rdfs:label ?label.
       |	?company owl:sameAs ?sameCompany.
       |}
       |service <http://155.223.25.2:8890/stockmarket/sparql>{
       |	?company stockmarket:market ?market.
       |	?company stockmarket:currency ?currency.
       |  ?company stockmarket:stockPrice ?stockPrice.
       |}
       |}
       |""".stripMargin

  val VERY_HIGH_SELECTIVE_FEDERATED_QUERY =
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |PREFIX dbo: <http://dbpedia.org/ontology/>
       |PREFIX dbpedia: <http://dbpedia.org/resource/>
       |PREFIX nytimes: <http://data.nytimes.com/elements/>
       |PREFIX stockmarket: <http://stockmarket.com/elements/>
       |PREFIX owl: <http://www.w3.org/2002/07/owl#>
       |Select * where {
       |service <http://155.223.25.4:8890/dbpedia/sparql> {
       |  ?company rdf:type dbo:Company.
       |	?company dbo:numberOfStaff ?staffCount.
       |	?company owl:sameAs ?nytCompany.  FILTER (?staffCount>100 && ?staffCount<=1000)}
       |service <http://155.223.25.1:8890/nytimes/sparql> {
       |  ?nytCompany rdf:type nytimes:Company.
       |  ?nytCompany nytimes:reputation "Elite"^^xsd:string.
       |  ?nytCompany nytimes:associated_article_count ?articleCount.
       |}
       |service <http://155.223.25.2:8890/stockmarket/sparql>{
       |	?nytCompany rdf:type stockmarket:Company.
       |  ?nytCompany stockmarket:valueChange ?valueChange. FILTER (?valueChange>=10000&&?valueChange<=15000)
       |  ?nytCompany stockmarket:stockPrice ?stockPrice.
       |}
       |}
       |""".stripMargin

}
