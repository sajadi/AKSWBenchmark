package org.benchmark.ontology;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.helper.Constants;
import org.benchmark.helper.JDBC;
import org.benchmark.helper.MD5HashGenerator;
import org.openrdf.model.URI;

import java.io.FileOutputStream;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Sep 27, 2010
 * Time: 4:21:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class InstanceProcessor extends Thread{

    private static Logger logger = Logger.getLogger(InstanceProcessor.class);

    public InstanceProcessor(String name, int priority){
        this.setPriority(priority);
        this.setName(name);
        start();
    }

    public InstanceProcessor(String name){
        this(name, Thread.NORM_PRIORITY);
    }

    public InstanceProcessor(){
        this("InstanceProcessor", Thread.NORM_PRIORITY);
    }

    public void run(){
        System.out.println("run is called");
        while(true){
//            System.out.println("inside loop");
            if(!OntologyClassInstancesLoader.nodesToBeTraversedQueue.isEmpty()){
//                System.out.println("inside if");
                breadthFirstSearch();

            }
        }
    }

    public void breadthFirstSearch()
    {
        while(!OntologyClassInstancesLoader.nodesToBeTraversedQueue.isEmpty())
        {
            RDFNode node = OntologyClassInstancesLoader.nodesToBeTraversedQueue.remove();
            logger.info("Node " + node.toString() + " has been eliminated, by thread " + this.getName());

            //TODO Check the dequeued resource and if it starts with http://dbpedia.org/resource then flag it in db
            //TODO as selected
            try{
                //Hashcode of the node
                String strHashcode = MD5HashGenerator.getMD5HashCode(node.toString());

                //Getting the node details from Instance table, i.e. if the node exists, this means that it is a class
                //instance
                String getNodeInfoStmt = "SELECT * FROM " + Constants.INSTANCE_TABLENAME + " WHERE " + Constants.FIELD_INSTANCE_URI +
                        " = '" + node.toString() + "'";

                JDBC jdbc = JDBC.getDefaultConnection();
                java.sql.ResultSet rs = jdbc.exec(getNodeInfoStmt);

                if(rs.next()){ //Node is a class instance
                    System.out.println("It's a class instance");

                    boolean isInstanceSelectedBefore = false;
                    isInstanceSelectedBefore = rs.getBoolean(Constants.FIELD_IS_SELECTED);
                    rs.close();
                    //If the class instance is already selected before, i.e. it is already visited, then we just move on;
                    if(isInstanceSelectedBefore)
                        continue;

                    //Setting the selected field of the node to true, to indicate that it is visited now
                    String setNodeSelected = "UPDATE " + Constants.INSTANCE_TABLENAME + " SET "+  Constants.FIELD_IS_SELECTED +" = 1 WHERE "
                            + Constants.FIELD_INSTANCE_URI + " = ?" ;
                    PreparedStatement prepStmt = jdbc.prepare(setNodeSelected);
                    jdbc.executeStatement(prepStmt, new String[]{node.toString()});
//                    jdbc.executeStatement(prepStmt, new String[]{"http://dbpedia.org/resource/%C3%9CberSoldier"});
                }
                else{//Node is not a class instance, so we should look at the visited table, to determine whether it is
                     //already visited or not
                     //Look at the Visited table, to determine if the node already there or not
                    String getNodeVisitedStatus = "SELECT * FROM " + Constants.VISITED_TABLENAME + " WHERE " + Constants.FIELD_NODE_URI +
                            " = '" + node.toString() + "'";

                    rs = jdbc.exec(getNodeVisitedStatus);
                    if(rs.next()){//Node already visited before, so we just move on
                        rs.close();
                        continue;
                    }

                    //The node is not visited before, so we should insert it into the table, to indicate that it is visited
                    String strInsertNodeStmt = "INSERT INTO " + Constants.VISITED_TABLENAME + " VALUES(?, ?)";
                    PreparedStatement prepStmt = jdbc.prepare(strInsertNodeStmt);
                    jdbc.executeStatement(prepStmt, new String[]{strHashcode, node.toString()});
                }

                logger.info("The node " + node.toString() + " has been visited");
                System.out.println("Before getUnvisitedChildNodes");
                getUnvisitedChildNodes(node);
                System.out.println("after getUnvisitedChildNodes");

            }
            catch (Exception exp){
                logger.error("Processing of node " + node.toString() + " failed", exp);
            }

        }

    }

    /**
     * Determines whether the node is selected before or not
     * @param node  The node to be checked
     * @return  The status of the node i.e. Visited or not
     */
    private boolean isNodeVisitedBefore(RDFNode node){
        boolean nodeVisiteddBefore = false;
        //Hashcode of the node
        String strHashcode = MD5HashGenerator.getMD5HashCode(node.toString());
        try{
            //Getting the node details from Instance table, i.e. if the node exists, this means that it is a class
            //instance
            String getNodeInfoStmt = "SELECT * FROM " + Constants.INSTANCE_TABLENAME + " WHERE " + Constants.FIELD_INSTANCE_HASH +
                    " = '" + strHashcode + "'";

            JDBC jdbc = JDBC.getDefaultConnection();
            java.sql.ResultSet rs = jdbc.exec(getNodeInfoStmt);
            if(rs.next()){ //Node is a class instance
                nodeVisiteddBefore = rs.getBoolean(Constants.FIELD_IS_SELECTED);
                rs.close();
            }

            //Node is not a class instance, so we should look at the visited table, to determine whether it is
            //already visited or not
            //Look at the Visited table, to determine if the node already there or not
            String getNodeVisitedStatus = "SELECT * FROM " + Constants.VISITED_TABLENAME + " WHERE " + Constants.FIELD_URI_HASH +
                    " = '" + strHashcode + "'";

            rs = jdbc.exec(getNodeVisitedStatus);
             if(rs.next()){//Node already visited before, so we just move on
                 rs.close();
                nodeVisiteddBefore = true;
             }
        }
        catch (Exception exp){
            logger.error("Node visited status cannot be determined due to " + exp.getMessage(), exp);
             nodeVisiteddBefore = false;
        }
        return nodeVisiteddBefore;
    }

    private void getUnvisitedChildNodes(RDFNode subjectNode){
        ArrayList<URI> nodesList = new ArrayList<URI>();

        //Get all statements related to the passed subject
        String subjectRelatedTriplesQuery = String.format("CONSTRUCT {?s  ?p ?o} FROM <http://dbpedia.org>  WHERE { {?s ?p ?o} " +
                    "FILTER (?s = <%s>)}", subjectNode);

        QueryEngineHTTP queryExecuter = null;

        queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, subjectRelatedTriplesQuery);

        Model testModel =  queryExecuter.execConstruct();

        //We should copy the statements of the model to an auxiliary array at first, in order to be able to iterate through
        //it, as we cannot iterate through the model itself as we will remove items from it
        List<Statement> modelStmtList = new ArrayList<Statement>();
        StmtIterator iteratorForStatements = testModel.listStatements();
        while(iteratorForStatements.hasNext()){
            Statement currentStatement = iteratorForStatements.next();
            modelStmtList.add(currentStatement);
        }

        //Iterate through the returned triples, and whenever another URI is encountered as object, we will call this
        // function recursively
        for(Statement currentStatement: modelStmtList){
            //If the object of the statement is a URI, then we should add the statement and recursively iterate through
            //the other statements related to it.
            String MD5Hashcode = MD5HashGenerator.getMD5HashCode(currentStatement);

//            if((currentStatement.getObject().isResource()) && !(uriLinkedTriples.containsKey(MD5Hashcode))){
            boolean nodeVisited = isNodeVisitedBefore(currentStatement.getObject());
            if((currentStatement.getObject().isResource()) && !nodeVisited){
                RDFNode objectNode = currentStatement.getObject();
//                uriLinkedTriples.put(MD5Hashcode, currentStatement);
                OntologyClassInstancesLoader.nodesToBeTraversedQueue.add(objectNode);
//                visitedNodes.put(MD5HashGenerator.getMD5HashCode(objectNode.toString()), objectNode);
                System.out.println("Node " + objectNode.toString() + " is inserted to queue, and queue size = " +
                        OntologyClassInstancesLoader.nodesToBeTraversedQueue.size());
            }
            else if (currentStatement.getObject().isResource() && nodeVisited) {
                testModel.remove(currentStatement);
                System.out.println("Already visited");
            }
        }
        System.out.println(testModel.size());
        _writeModelToFile(testModel);
//        return nodesList;
    }

    private void _writeModelToFile(Model modelToBeWritten) {
        try{
            modelToBeWritten.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
            logger.info("Model is successfully written to file");
        }
        catch (Exception exp){
            logger.error("Model cannot be written to file due to " + exp.getMessage(), exp);
        }
    }
}
