package querying.transformation

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import com.hp.hpl.jena.vocabulary.RDF
import querying.main.{Constants, QueryingUtils}
import querying.message.Result

object RedisTransformer {


  /**
   * ITEMID --> [LABEL, ABBREVIATION, DBSOURCE, LINKSTO, CATEGORY, UNITNAME, PARAM_TYPE, CONCEPTID]
   */
  private def create_ontology_for_d_item(model: Model, key: String, propertyValueList: Option[List[Option[String]]]) = {
    val itemRsc = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + "d_item/" + key)
    model.add(itemRsc, Constants.ITEM_ID_PRP, key, XSDDatatype.XSDint)
    model.add(itemRsc, Constants.LABEL_PRP, propertyValueList.get.head.get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.ABBREVIATION_PRP, propertyValueList.get(1).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.DB_SOURCE_PRP, propertyValueList.get(2).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.LINKS_TO_PRP, propertyValueList.get(3).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.CATEGORY_PRP, propertyValueList.get(4).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.UNIT_NAME_PRP, propertyValueList.get(5).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.PARAM_TYPE_PRP, propertyValueList.get(6).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.CONCEPT_ID_PRP, propertyValueList.get(7).get, XSDDatatype.XSDint)
    model.add(itemRsc, RDF.`type`, Constants.MIMIC_D_ITEM_CLS)
  }


  /**
   * ITEMID --> [LABEL, FLUID, CATEGORY, LOINC_CODE]
   */
  private def create_ontology_for_d_lab_item(model: Model, key: String, propertyValueList: Option[List[Option[String]]]) = {
    val itemRsc = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + "d_lab_item/" + key)
    model.add(itemRsc, Constants.ITEM_ID_PRP, key, XSDDatatype.XSDint)
    model.add(itemRsc, Constants.LABEL_PRP, propertyValueList.get.head.get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.FLUID_PRP, propertyValueList.get(1).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.CATEGORY_PRP, propertyValueList.get(2).get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.LOINC_CODE_PRP, propertyValueList.get(3).get, XSDDatatype.XSDstring)
    model.add(itemRsc, RDF.`type`, Constants.MIMIC_D_LAB_ITEM_CLS)
  }


  /**
   * ICD9_CODE --> [SHORT_TITLE, LONG_TITLE]
   */
  private def create_ontology_for_d_icd_procedures(model: Model, key: String, propertyValueList: Option[List[Option[String]]]) = {
    val itemRsc = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + "d_icd_procedure/" + key)
    model.add(itemRsc, Constants.ICD_9_CODE_PRP, key, XSDDatatype.XSDint)
    model.add(itemRsc, Constants.SHORT_TITLE_PRP, propertyValueList.get.head.get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.LONG_TITLE_PRP, propertyValueList.get(1).get, XSDDatatype.XSDstring)
    model.add(itemRsc, RDF.`type`, Constants.MIMIC_D_ICD_PROCEDURE_CLS)
  }

  /**
   * ICD9_CODE --> [SHORT_TITLE,	LONG_TITLE]
   */
  def create_ontology_for_d_icd_diagnoses(model: Model, key: String, propertyValueList: Option[List[Option[String]]]) = {
    val itemRsc = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + "d_icd_diagnose/" + key)
    model.add(itemRsc, Constants.ICD_9_CODE_PRP, key, XSDDatatype.XSDint)
    model.add(itemRsc, Constants.SHORT_TITLE_PRP, propertyValueList.get.head.get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.LONG_TITLE_PRP, propertyValueList.get(1).get, XSDDatatype.XSDstring)
    model.add(itemRsc, RDF.`type`, Constants.MIMIC_D_ICD_DIAGNOSE_CLS)
  }

  /**
   * CGID -->	[LABEL,	DESCRIPTION]
   */
  def create_ontology_for_caregivers(model: Model, key: String, propertyValueList: Option[List[Option[String]]]) = {
    val itemRsc = ResourceFactory.createResource(Constants.MIMIC_RESOURCE_URI + "caregiver/" + key)
    model.add(itemRsc, Constants.CGID_PRP, key, XSDDatatype.XSDint)
    model.add(itemRsc, Constants.LABEL_PRP, propertyValueList.get.head.get, XSDDatatype.XSDstring)
    model.add(itemRsc, Constants.DESCRIPTION_PRP, propertyValueList.get(1).get, XSDDatatype.XSDstring)
    model.add(itemRsc, RDF.`type`, Constants.MIMIC_CAREGIVER_CLS)
  }

  def create_ontology_for_procedures_icd(model: Model, key: String, propertyValueList: Option[List[Option[String]]]) = {
    None
  }

  def create_ontology_for_diagnoses_icd(model: Model, key: String, propertyValueList: Option[List[Option[String]]]) = {
    None
  }

  def transformToRdfResult(database: Int, resultMap: Map[String, Option[Any]]): Option[Result] = {
    var query: String = ""

    val model = ModelFactory.createDefaultModel()
    for ((key, redisResult) <- resultMap) {
      val propertyValueList = redisResult.asInstanceOf[Option[List[Option[String]]]]
      database match {
        case 0 =>
          create_ontology_for_d_item(model, key, propertyValueList)
          query = Constants.D_ITEM_QUERY
        case 1 =>
          create_ontology_for_d_lab_item(model, key, propertyValueList)
          query = Constants.D_LAB_ITEM_QUERY
        case 2 =>
          create_ontology_for_d_icd_procedures(model, key, propertyValueList)
          query = Constants.D_ICD_PROCEDURE_QUERY
        case 3 =>
          create_ontology_for_d_icd_diagnoses(model, key, propertyValueList)
          query = Constants.D_ICD_DIAGNOSE_QUERY
        case 4 =>
          create_ontology_for_caregivers(model, key, propertyValueList)
          query = Constants.CAREGIVER_QUERY
        case 6 =>
          create_ontology_for_procedures_icd(model, key, propertyValueList)
          query = Constants.PROCEDURE_ICD_QUERY
        case 7 =>
          create_ontology_for_diagnoses_icd(model, key, propertyValueList)
          query = Constants.DIAGNOSE_ICD_QUERY
        case _ => println("Invalid database selected. (Valid through 0-7)")
      }
    }
    val result = QueryingUtils.convertRdf2Result(QueryExecutionFactory.create(query, model).execSelect())
    Option(result)


  }
}
