package querying.transformation

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.{ModelFactory, ResourceFactory}
import com.hp.hpl.jena.vocabulary.RDF
import querying.main.{Constants, QueryingUtils}
import querying.message.Result

object RedisTransformer {

  /**
   * ITEMID --> [LABEL, ABBREVIATION, DBSOURCE, LINKSTO, CATEGORY, UNITNAME, PARAM_TYPE, CONCEPTID]
   */
  def transform_d_items(resultMap: Map[String, Option[Any]]): Option[Result] = {
    val model = ModelFactory.createDefaultModel()
    for ((key, redisResult) <- resultMap) {
      val propertyValueList = redisResult.asInstanceOf[Option[List[Option[String]]]]
      val itemRsc = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + "d_item/" + key)
      val itemIdValue = key
      val labelValue = propertyValueList.get.head
      val abbreviationValue = propertyValueList.get(1)
      val dbsourceValue = propertyValueList.get(2)
      val linkstoValue = propertyValueList.get(3)
      val categoryValue = propertyValueList.get(4)
      val unitnameValue = propertyValueList.get(5)
      val paramtypeValue = propertyValueList.get(6)
      val conceptidValue = propertyValueList.get(7)

      model.add(itemRsc, Constants.ITEM_ID_PRP, itemIdValue, XSDDatatype.XSDint)
      model.add(itemRsc, Constants.LABEL_PRP, labelValue.get, XSDDatatype.XSDstring)
      model.add(itemRsc, Constants.ABBREVIATION_PRP, abbreviationValue.get, XSDDatatype.XSDstring)
      model.add(itemRsc, Constants.DB_SOURCE_PRP, dbsourceValue.get, XSDDatatype.XSDstring)
      model.add(itemRsc, Constants.LINKS_TO_PRP, linkstoValue.get, XSDDatatype.XSDstring)
      model.add(itemRsc, Constants.CATEGORY_PRP, categoryValue.get, XSDDatatype.XSDstring)
      model.add(itemRsc, Constants.UNIT_NAME_PRP, unitnameValue.get, XSDDatatype.XSDstring)
      model.add(itemRsc, Constants.PARAM_TYPE_PRP, paramtypeValue.get, XSDDatatype.XSDstring)
      model.add(itemRsc, Constants.CONCEPT_ID_PRP, conceptidValue.get, XSDDatatype.XSDint)
      model.add(itemRsc, RDF.`type`, Constants.MIMIC_D_ITEM)
    }
    val queryStr =
      s"""
         |PREFIX mimic-rsc:<${Constants.MIMIC_RESOURCE_URI}>
         |PREFIX mimic-ont:<${Constants.MIMIC_ONTOLOGY_URI}>
         |SELECT * WHERE {
         |?itemRsc mimic-ont:itemid ?itemid.
         |?itemRsc mimic-ont:label ?label.
         |?itemRsc mimic-ont:abbreviation ?abbreviation.
         |?itemRsc mimic-ont:dbsource ?dbsource.
         |?itemRsc mimic-ont:linksto ?linksto.
         |?itemRsc mimic-ont:category ?category.
         |?itemRsc mimic-ont:unitname ?unitname.
         |?itemRsc mimic-ont:paramtype ?paramtype.
         |?itemRsc mimic-ont:conceptid ?conceptid.
         |}""".stripMargin
    val result = QueryingUtils.convertRdf2Result(QueryExecutionFactory.create(queryStr, model).execSelect())
    Option(result)
  }

  def transform_d_lab_items(resultMap: Map[String, Option[Any]]): Option[Result] = {
    None
  }

  def transform_d_icd_procedures(resultMap: Map[String, Option[Any]]): Option[Result] = {
    None
  }

  def transform_d_icd_diagnoses(resultMap: Map[String, Option[Any]]): Option[Result] = {
    None
  }

  def transform_caregivers(resultMap: Map[String, Option[Any]]): Option[Result] = {
    None
  }

  def transform_drg_codes(resultMap: Map[String, Option[Any]]): Option[Result] = {
    None
  }

  def transform_procedures_icd(resultMap: Map[String, Option[Any]]): Option[Result] = {
    None
  }

  def transform_diagnoses_icd(resultMap: Map[String, Option[Any]]): Option[Result] = {
    None
  }

  def transformToRdfResult(database: Int, resultMap: Map[String, Option[Any]]): Option[Result] = {
    database match {
      case 0 => transform_d_items(resultMap)
      case 1 => transform_d_lab_items(resultMap)
      case 2 => transform_d_icd_procedures(resultMap)
      case 3 => transform_d_icd_diagnoses(resultMap)
      case 4 => transform_caregivers(resultMap)
      case 5 => transform_drg_codes(resultMap)
      case 6 => transform_procedures_icd(resultMap)
      case 7 => transform_diagnoses_icd(resultMap)
      case _ => None
    }

  }
}
