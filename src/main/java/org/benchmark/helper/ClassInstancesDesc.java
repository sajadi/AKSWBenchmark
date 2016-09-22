package org.benchmark.helper;

import org.apache.log4j.Logger;
import org.dbpedia.extraction.ontology.OntologyClass;
import org.openrdf.model.URI;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Sep 21, 2010
 * Time: 12:17:13 PM
 * Holds the list of class instances along with the number of instances that should be selected.
 */

public class ClassInstancesDesc implements Serializable{
    private static Logger logger = Logger.getLogger(ClassInstancesDesc.class);

    public String ontologyClass;
    private List<URI> instancesList;

    //This list is used to hold a list of the remaining instances that are not yet visited in order to help
    //in the process of selecting an instance at random
    private List<URI> remainingInstancesList;//

    private int totalNumberOfInstances;
    private int requiredNumberOfInstances;

    public int gerTotalNumberOfInstances(){
        return instancesList.size();
    }

    public int getRequiredNumberOfInstances(){
        return Math.round((BenchmarkConfigReader.percentageOfDataRequired /1000)
                            * instancesList.size());//The required number of instances to be selected
    }

    public ClassInstancesDesc(OntologyClass ontoClass, List<URI> instances){
        ontologyClass = ontoClass.uri();
        instancesList = instances;
        totalNumberOfInstances = instancesList.size();
        requiredNumberOfInstances = Math.round((BenchmarkConfigReader.percentageOfDataRequired /100)
                            * totalNumberOfInstances);//The required number of instances to be selected
    }

    public List<URI> getInstancesList(){
        return instancesList;
    }

    public void setInstancesList(List<URI> instList){
        instancesList = instList;
        remainingInstancesList = new ArrayList<URI>();
        for(URI instanceURI: instancesList)
                remainingInstancesList.add(instanceURI);
//        Collections.copy(remainingInstancesList, instancesList);//Copy the list to the other list that will be use in the randomization
        
        totalNumberOfInstances = instancesList.size();
        requiredNumberOfInstances = Math.round((BenchmarkConfigReader.percentageOfDataRequired /100)
                            * totalNumberOfInstances);//The required number of instances to be selected
    }

    /**
     * Selects an instance at random from the list of instances
     * @return  The URI of a random instance
     */
    public URI getRandomInstance(){
        //If the number
        int recnumber = getRequiredNumberOfInstances();
        int selnumber = getNumberOfSelectedInstances();
        
//        if(getNumberOfSelectedInstances() >= getRequiredNumberOfInstances())
        if(selnumber >= recnumber)
            return null;

        Random generator = new Random();
        int selectedInstanceIndex = generator.nextInt(remainingInstancesList.size());
        URI selectedInstanceURI = remainingInstancesList.remove(selectedInstanceIndex);
        return selectedInstanceURI;
    }

    /**
     * Returns the number of instance that are selected (visited) from the ontology class of this  object
     * @return  The number of instances that are selected (visited) before
     */
    public int getNumberOfSelectedInstances(){
        int numberOfSelectedInstances = 0;

        JDBC jdbc = JDBC.getDefaultConnection();
        String strCountNumberOfSelectedInstancesStmt = "SELECT COUNT(*) FROM " + Constants.INSTANCE_TABLENAME + " WHERE " +
                Constants.FIELD_IS_SELECTED + " = " + "1 AND " + Constants.FIELD_CLASS_ID +
                " = (SELECT " + Constants.FIELD_CLASS_ID +" FROM " + Constants.ONTOLOGY_CLASS_TABLENAME + " WHERE " +
                Constants.FIELD_CLASS_URI + " = '" + ontologyClass + "')";

        ResultSet rs = jdbc.exec(strCountNumberOfSelectedInstancesStmt);
        try{
            if(rs.next())
                numberOfSelectedInstances = rs.getInt(1);
            rs.close();
        }
        catch (Exception exp){
            logger.error("Ontology class " + ontologyClass +" is not found in the database");
        }
        return numberOfSelectedInstances;
    }
}
