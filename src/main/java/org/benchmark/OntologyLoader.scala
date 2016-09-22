package org.benchmark


/**
 * Created by IntelliJ IDEA.
 * User: mabrouk
 * Date: Aug 11, 2010
 * Time: 2:30:05 PM
 * To change this template use File | Settings | File Templates.
 */
import java.net.URL

import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.ontology.OntologyClass
import org.dbpedia.extraction.sources.WikiSource
import org.dbpedia.extraction.wikiparser.{Namespace, WikiTitle}

import scala.collection.mutable.ArrayBuffer


object OntologyLoader{
  def loadOntologyClasses(): List[OntologyClass] = {
    System.out.println("Loading ontology classes");

    val ontologySource = WikiSource.fromNamespaces(namespaces = Set( Namespace.OntologyClass, Namespace.OntologyProperty),
                                                   url = new URL("http://mappings.dbpedia.org/api.php"),
                                                   language = Language.Commons );

    val rdr = new OntologyReader();
    //rdr.read(ontologySource);
    val result = rdr.read(ontologySource).classes;
//    println("Ontolgy Classes = " + ontologySource);
    result.values.toList
  }                                                  
  
}