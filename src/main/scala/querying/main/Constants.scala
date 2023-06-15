package querying.main

import com.hp.hpl.jena.rdf.model.{Property, Resource, ResourceFactory}

object Constants {
  val DIAGNOSE_ICD_QUERY: String = ???

  val PROCEDURE_ICD_QUERY: String = ???


  val ACTOR_COUNT = "actor-count"
  val QUERY_COUNT = "query-count"
  val Kilobytes = 1024L
  val Megabytes: Long = Kilobytes * 1024L
  val Gigabytes: Long = Megabytes * 1024L
  val REDIS = "Redis"
  val POSTGRESQL = "Postgresql"
  val INFLUXDB = "Influxdb"
  val ELASTICSEARCH = "Elasticsearch"
  val MIMIC_BASE_URI = "https://mimic.mit.edu/"
  val MIMIC_ONTOLOGY_URI: String = MIMIC_BASE_URI + "ontology/"
  val MIMIC_RESOURCE_URI: String = MIMIC_BASE_URI + "resource/"

  val MIMIC_D_ITEM_CLS: Resource = ResourceFactory.createResource(MIMIC_ONTOLOGY_URI + "D_Item")
  val MIMIC_D_LAB_ITEM_CLS: Resource = ResourceFactory.createResource(MIMIC_ONTOLOGY_URI + "D_Lab_Item")
  val MIMIC_D_ICD_PROCEDURE_CLS: Resource = ResourceFactory.createResource(MIMIC_ONTOLOGY_URI + "D_Icd_Procedure")
  val MIMIC_D_ICD_DIAGNOSE_CLS: Resource = ResourceFactory.createResource(MIMIC_ONTOLOGY_URI + "D_Icd_Diagnose")
  val MIMIC_CAREGIVER_CLS: Resource = ResourceFactory.createResource(MIMIC_ONTOLOGY_URI + "Caregiver")

  val ITEM_ID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "itemid")
  val LABEL_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "label")
  val ABBREVIATION_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "abbreviation")
  val DB_SOURCE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "dbsource")
  val LINKS_TO_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "linksto")
  val CATEGORY_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "category")
  val UNIT_NAME_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "unitname")
  val PARAM_TYPE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "paramtype")
  val CONCEPT_ID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "conceptid")
  val FLUID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "fluid")
  val LOINC_CODE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "loinc_code")
  val ICD_9_CODE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "icd9_code")
  val LONG_TITLE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "long_title")
  val SHORT_TITLE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "short_title")
  val DESCRIPTION_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "description")
  val CGID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "cgid")
  val HADM_ID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "hadm_id")
  val SUBJECT_ID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "subject_id")

  val D_ITEM_QUERY: String =
    s"""
       |PREFIX mimic-rsc:<${MIMIC_RESOURCE_URI}>
       |PREFIX mimic-ont:<${MIMIC_ONTOLOGY_URI}>
       |SELECT * WHERE {
       |?d_item mimic-ont:itemid ?itemid.
       |?d_item mimic-ont:label ?label.
       |?d_item mimic-ont:abbreviation ?abbreviation.
       |?d_item mimic-ont:dbsource ?dbsource.
       |?d_item mimic-ont:linksto ?linksto.
       |?d_item mimic-ont:category ?category.
       |?d_item mimic-ont:unitname ?unitname.
       |?d_item mimic-ont:paramtype ?paramtype.
       |?d_item mimic-ont:conceptid ?conceptid.
       |}""".stripMargin

  val D_LAB_ITEM_QUERY: String =
    s"""
       |PREFIX mimic-rsc:<${MIMIC_RESOURCE_URI}>
       |PREFIX mimic-ont:<${MIMIC_ONTOLOGY_URI}>
       |SELECT * WHERE {
       |?d_lab_item mimic-ont:itemid ?itemid.
       |?d_lab_item mimic-ont:label ?label.
       |?d_lab_item mimic-ont:fluid ?fluid.
       |?d_lab_item mimic-ont:category ?category.
       |?d_lab_item mimic-ont:loinc_code ?loinc_code.
       |}""".stripMargin

  val D_ICD_PROCEDURE_QUERY: String =
    s"""
       |PREFIX mimic-rsc:<${MIMIC_RESOURCE_URI}>
       |PREFIX mimic-ont:<${MIMIC_ONTOLOGY_URI}>
       |SELECT * WHERE {
       |?d_icd_procedure mimic-ont:icd9_code ?icd9_code.
       |?d_icd_procedure mimic-ont:short_title ?short_title.
       |?d_icd_procedure mimic-ont:long_title ?long_title.
       |}""".stripMargin

  val D_ICD_DIAGNOSE_QUERY: String =
    s"""
       |PREFIX mimic-rsc:<${MIMIC_RESOURCE_URI}>
       |PREFIX mimic-ont:<${MIMIC_ONTOLOGY_URI}>
       |SELECT * WHERE {
       |?d_icd_diagnose mimic-ont:icd9_code ?icd9_code.
       |?d_icd_diagnose mimic-ont:short_title ?short_title.
       |?d_icd_diagnose mimic-ont:long_title ?long_title.
       |}""".stripMargin

  val CAREGIVER_QUERY: String =
    s"""
       |PREFIX mimic-rsc:<${MIMIC_RESOURCE_URI}>
       |PREFIX mimic-ont:<${MIMIC_ONTOLOGY_URI}>
       |SELECT * WHERE {
       |?caregiver mimic-ont:cgid ?cgid.
       |?caregiver mimic-ont:label ?label.
       |?caregiver mimic-ont:description ?description.
       |}""".stripMargin

}
