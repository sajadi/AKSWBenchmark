package org.benchmark.ontology;

import org.apache.log4j.Logger;
import org.benchmark.OntologyLoader;
import org.benchmark.helper.BenchmarkConfigReader;
import org.dbpedia.extraction.ontology.OntologyClass;
import scala.collection.*;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Aug 11, 2010
 * Time: 5:55:46 PM
 * This class is responsible for handling the ontology classes that will be used in the data generation process  
 */

public class OntologyClassesSelector {

    private static Logger logger = Logger.getLogger(OntologyClassesSelector.class);

    /**
     * Loads the all ontology class found in
     * @return
     */
    public List<OntologyClass> loadAllOntologyClasses(){

        List <OntologyClass> ontologyClassList = JavaConverters.seqAsJavaList(OntologyLoader.loadOntologyClasses());

        int totalNumberOfClasses = ontologyClassList.size();
        int classNumber = 1;

        System.out.println("Classes = " + ontologyClassList);

        List <OntologyClass> neededClasses = _removeUnneededClasses(ontologyClassList);

        logger.info("All ontology classes are successfully loaded");
        return neededClasses;
    }

    /**
     * Returns only a subset of all classes in the system
     * @return  A list containing subset of ontology classes
     */
    public List<OntologyClass> loadSubsetOntologyClasses() throws InvalidParameterException{

        if((BenchmarkConfigReader.percentageOfClassesConsidered<=0) || (BenchmarkConfigReader.percentageOfClassesConsidered>100))
            throw new InvalidParameterException("Invalid percentage passed");
        
        List <OntologyClass> allOntologyClassesList = new ArrayList<OntologyClass>
                                                (JavaConverters.seqAsJavaList(OntologyLoader.loadOntologyClasses()));

        List <OntologyClass> neededClasses = _removeUnneededClasses(allOntologyClassesList);

        logger.info("All ontology classes are successfully loaded");

        //The list that will contain the output
        List <OntologyClass> selectedOntologyClassesList = new ArrayList<OntologyClass>();
        int totalNumberOfClasses = neededClasses.size();//Total number of all classes
        int requiredNumberOfClasses = Math.round((BenchmarkConfigReader.percentageOfClassesConsidered/100)
                * totalNumberOfClasses);//Required number of classes to be generated

        Random generator = new Random();
        int currentClassNumber = 0;

        //We generate a random number which coincides with the list boundaries, and then remove that element from the large
        //list and place it in the output list
        while((currentClassNumber < requiredNumberOfClasses) && (!neededClasses.isEmpty())){

            int selectedClassNumber = generator.nextInt(neededClasses.size());

            OntologyClass selectedOntologyClass = neededClasses.remove(selectedClassNumber);
            selectedOntologyClassesList.add(selectedOntologyClass);
            currentClassNumber++;
        }

        logger.info(BenchmarkConfigReader.percentageOfClassesConsidered + "% of ontology classes are produced");
        return selectedOntologyClassesList;
    }

    /**
     * Removes the unnedeed ontology classes e.g. "http://xmlns.com/foaf/0.1/Person and "http://www.w3.org/2002/07/owl#Thing"
     * @param allOntologyClasses    A list containing all ontology classes
     * @return  A filtered list containing only DBpedia ontology classes
     */
    private List <OntologyClass> _removeUnneededClasses(List<OntologyClass> allOntologyClasses) {

        List <OntologyClass> correctClassList = new ArrayList<OntologyClass>();

        for(OntologyClass ontoClass: allOntologyClasses){
            if(ontoClass.uri().contains("http://dbpedia.org"))
                correctClassList.add(ontoClass);
        }
        return correctClassList;
    }

}
