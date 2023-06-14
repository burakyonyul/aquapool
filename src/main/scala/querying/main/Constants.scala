package querying.main

import com.hp.hpl.jena.rdf.model.{Property, Resource, ResourceFactory}

object Constants {
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

  val MIMIC_D_ITEM_URI: String = MIMIC_ONTOLOGY_URI + "D_Item"

  val MIMIC_D_ITEM: Resource = ResourceFactory.createResource(MIMIC_D_ITEM_URI)
  val ITEM_ID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "itemid")
  val LABEL_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "label")
  val ABBREVIATION_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "abbreviation")
  val DB_SOURCE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "dbsource")
  val LINKS_TO_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "linksto")
  val CATEGORY_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "category")
  val UNIT_NAME_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "unitname")
  val PARAM_TYPE_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "paramtype")
  val CONCEPT_ID_PRP: Property = ResourceFactory.createProperty(MIMIC_ONTOLOGY_URI + "conceptid")

}
