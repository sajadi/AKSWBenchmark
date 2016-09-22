package org.benchmark.ontology;

import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import org.apache.log4j.Logger;
import org.benchmark.helper.ClassInstancesDesc;
import org.benchmark.helper.Constants;
import org.benchmark.helper.JDBC;
import org.openrdf.model.URI;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Sep 28, 2010
 * Time: 11:20:00 PM
 * Responsible for operations on ontology class instances
 */
public class InstanceSelector {

    private static Logger logger = Logger.getLogger(InstanceSelector.class);
    public ArrayList<ClassInstancesDesc> classInstancesArray = new ArrayList<ClassInstancesDesc>();

    private int highestInstanceNumber = 0;

    /**
     * The instances of ontology classes are saved in database, so we can get a random instance from the database
     * @return  Random ontology instance
     */
    public RDFNode getRandomInstanceFromDatabase(){
        if(highestInstanceNumber<=0)
            readHighestInstanceNumber();

        Random generator = new Random();
        int selectedInstanceIndex = generator.nextInt(highestInstanceNumber);
        String instanceURI = getInstanceWithIndex(selectedInstanceIndex);

        if(instanceURI != "")
            return new ResourceImpl(instanceURI);
        else
            return null;
    }

    /**
     * The instances of ontology classes are also saved in file, so we can get a random instance from the file too
     * @return  Random ontology instance
     */
    public RDFNode getRandomInstanceFromFile(){

        if(isAllInstancesRemoved())
            return null;

        Random generator = new Random();

        //Since classInstancesArray is an Arraylist adn it in turn contains another Arrayist
        //we should generate 2 random numbers, one for the class and the other is for the instance
        int classIndex, instanceIndex;
        classIndex = generator.nextInt(classInstancesArray.size());

        //We should use a loop because may the class that was selected contains no instances at all, so we should try to find another class
        while(classInstancesArray.get(classIndex).getInstancesList().size() <= 0)
            classIndex = generator.nextInt(classInstancesArray.size());

        instanceIndex = generator.nextInt(classInstancesArray.get(classIndex).getInstancesList().size());

//        logger.info("selected class index = " + classIndex + ", and selected instance index = " + instanceIndex);
//        logger.info("number of classes = " + classInstancesArray.size() +
//                ", and number of instances = " + classInstancesArray.get(classIndex).getInstancesList().size());

        //The selected instance must be removed in order not to be selected again
        URI uri = classInstancesArray.get(classIndex).getInstancesList().remove(instanceIndex);
        return new ResourceImpl(uri.toString());
    }

    /**
     * This function is very important, as when we select an instance at random, may all instances be already processed
     * so we cannot continue
     * @return  True if all instances were alreday removed
     */
    private boolean isAllInstancesRemoved(){
        for(ClassInstancesDesc desc :classInstancesArray){
            if(desc.getInstancesList().size()>0)
                return false;
        }
        return true;
    }

    /**
     * Reads an instance from the database whose index is equal to index passed
     * @param selectedInstanceIndex The index of the instance that should be fetched from database
     * @return  String containing the URI of the instance with passed index
     */
    private String getInstanceWithIndex(int selectedInstanceIndex) {
        String uri = "";
        JDBC jdbc = JDBC.getDefaultConnection();
        try{
            String selectInstanceStmt = "SELECT * FROM " + Constants.INSTANCE_TABLENAME + " WHERE " +
                    Constants.FIELD_INSTANCE_INDEX + " = " + selectedInstanceIndex;
            ResultSet rs = jdbc.exec(selectInstanceStmt);

            if(rs.next())
                uri = rs.getString(Constants.FIELD_INSTANCE_URI);
                logger.info("Instance with index " + selectedInstanceIndex + " is successfully fetched");
            rs.close();
        }
        catch(Exception exp){
            logger.error("Instance with index " + selectedInstanceIndex + " cannot be fetched", exp);
        }

        return uri;
    }

    /**
     * Reads that highest instance number from database in order to control the randomization process
     */
    private void readHighestInstanceNumber(){
        JDBC jdbc = JDBC.getDefaultConnection();
        try{
            String maxInstanceStmt = "SELECT MAX(" + Constants.FIELD_INSTANCE_INDEX + ") FROM " +
                    Constants.INSTANCE_TABLENAME;
            ResultSet rs = jdbc.exec(maxInstanceStmt);

            if(rs.next())
                highestInstanceNumber = rs.getInt(1);
                logger.info("Highest instance is successfully fetched");
            rs.close();
        }
        catch(Exception exp){
            logger.error("Highest instance cannot be fetched", exp);
        }
    }

    /**
     * Loads the classes along with their instances from a file that is originally built using serialization
     * @param strFileName   The filename
     */
    public void loadClassInstancesFromFile(String strFileName){
        FileInputStream fis = null;
        ObjectInputStream in = null;
        logger.info("Read instances from file started");
        try {
            fis = new FileInputStream(strFileName);
            in = new ObjectInputStream(fis);
            classInstancesArray = (ArrayList<ClassInstancesDesc>) in.readObject();
            in.close();
        }
        catch (Exception exp) {
            logger.error("Instance cannot be read from file", exp);
            return;
        }

        int total=0;
        for(int classIndex=0; classIndex<classInstancesArray.size(); classIndex++){
                ClassInstancesDesc desc = classInstancesArray.get(classIndex);
            total+=desc.getInstancesList().size();
        }
        logger.info(total + " instances are successfully loaded");
    }

    /**
     * Writes the classes along with their instances to a file using serialization
     * @param strFileName   The filename
     */
    public void writeClassInstancesToFile(String strFileName){
    try{
            FileOutputStream fos = new FileOutputStream(strFileName);
		    ObjectOutputStream out = new ObjectOutputStream(fos);
            out.writeObject(classInstancesArray);
            out.close();
            logger.info("Instance are successfully serialized");
        }
        catch (Exception exp){
            logger.error("Instances are cannot be serialized to file", exp);
        }
    }

}
