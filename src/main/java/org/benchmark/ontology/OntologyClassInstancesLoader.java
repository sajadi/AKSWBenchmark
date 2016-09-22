package org.benchmark.ontology;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.N3JenaWriter;
import com.hp.hpl.jena.n3.N3TurtleJenaWriter;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import org.apache.log4j.Logger;
import org.benchmark.helper.*;
import org.dbpedia.extraction.ontology.OntologyClass;
import org.openrdf.model.URI;
import org.openrdf.model.impl.*;
import com.hp.hpl.jena.graph.Graph;


import java.io.FileOutputStream;
import java.io.FileInputStream;

import org.openrdf.rio.turtle.*;


import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Aug 12, 2010
 * Time: 11:05:28 AM
 * This class is responsible for loading the instances of a specific ontology class.
 */
public class OntologyClassInstancesLoader {
    

//    private static final String sparqlEndpoint = "http://dbpedia.org/sparql";

    private static Logger logger = Logger.getLogger(OntologyClassInstancesLoader.class);

    //This array will hold the triples related to a specific uri, i.e. used to follow the links with DBpedia  
    private HashMap<String, Statement> uriLinkedTriples = new HashMap<String, Statement>();

    private HashMap<String, RDFNode> visitedNodes = new HashMap<String, RDFNode>(); //This hash map contains the visited resources

    public static Queue<RDFNode> nodesToBeTraversedQueue = new LinkedBlockingQueue<RDFNode>();

//    private ArrayList<List<URI>> classInstancesArray = new ArrayList<List<URI>>();
      public ArrayList<ClassInstancesDesc> classInstancesArray = new ArrayList<ClassInstancesDesc>();
    /**
     * Loads the instances of a specific ontology class
     * @param requiredClass The ontology class of interest
     * @return  A list containing the URIs of all instances of the passed class
     */
    public List<URI> getAllOntologyClassInstances(OntologyClass requiredClass){

        List<URI> instancesList = new ArrayList<URI>();
        int numberOfInstances;
        boolean needsOffset = false;

        do{
            numberOfInstances = 0;
            Query query = new Query();
            String ontologyClassInstancesQuery = "SELECT ?s from <http://dbpedia.org>  WHERE { ?s a <" + requiredClass + ">.}" ;
            if(needsOffset)
                ontologyClassInstancesQuery += " OFFSET " + instancesList.size();

            QueryEngineHTTP queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, ontologyClassInstancesQuery);

            try{
               ResultSet resultSet = queryExecuter.execSelect();
               //Iterate through returned instances and place them in the output list as URIs

               while (resultSet.hasNext()){
                   QuerySolution sol = resultSet.next();

                   URI instanceURI = new URIImpl(sol.get("?s").toString());

                   instancesList.add(instanceURI);
    //               logger.info(requiredClass.toString() + "             " + instanceURI);
                   numberOfInstances++;
               }
            }
            catch (Exception exp){
                logger.error("Unable to get the instances of a class, and the reason is " + exp.getMessage());
            }
            finally{
                queryExecuter.close();
            }
            needsOffset = true;
        }while(numberOfInstances == 2000);

        return instancesList;
    }

    public void loadClassInstancesFromFile(String strFileName){
        FileInputStream fis = null;
        ObjectInputStream in = null;
        try {
            fis = new FileInputStream(strFileName);
            in = new ObjectInputStream(fis);
            classInstancesArray = (ArrayList) in.readObject();
            in.close();
        } catch (Exception exp) {
            logger.error(exp);

        }

        int total=0;
        for(int classIndex=0; classIndex<classInstancesArray.size(); classIndex++){
                ClassInstancesDesc desc = classInstancesArray.get(classIndex);
            total+=desc.getInstancesList().size();
        }
        System.out.print(total + " instances are successfully loaded");
    }

    /**
     * Loads all instances of a given ontology class
     * @param requiredOntologyClass The ontology class for which the instances should be loaded
     * return   A description object containing a list of all instances along with number of already selected instances
     */
    public ClassInstancesDesc loadClassInstancesFromDatabase(OntologyClass requiredOntologyClass){

        ClassInstancesDesc desc = new ClassInstancesDesc(requiredOntologyClass, new ArrayList<URI>());

        JDBC jdbc = JDBC.getDefaultConnection();
        String strLoadInstancesStmt = "SELECT * FROM " + Constants.INSTANCE_TABLENAME + " WHERE " + Constants.FIELD_CLASS_ID +
                " = (SELECT " + Constants.FIELD_CLASS_ID +" FROM " + Constants.ONTOLOGY_CLASS_TABLENAME + " WHERE " +
                Constants.FIELD_CLASS_URI + " = '" + requiredOntologyClass.uri() + "')";

        java.sql.ResultSet rs = jdbc.exec(strLoadInstancesStmt);

        ArrayList<URI> Instances = new  ArrayList<URI>();

        try{
           while(rs.next()){
               Instances.add(new URIImpl(rs.getString(Constants.FIELD_INSTANCE_URI)));
//               desc.getInstancesList().add(new URIImpl(rs.getString(Constants.FIELD_INSTANCE_URI)));
               
//               if(rs.getBoolean(Constants.FIELD_IS_SELECTED))
//                    desc.numberOfSelectedInstances++;
           }
            rs.close();
        }
        catch(Exception exp){
            logger.error("Class instance cannot be extracted from database", exp);
        }
        finally {
            desc.setInstancesList(Instances);
            return desc;
        }
    }


    //Inserts all ontology classes along with the instances of each class into database  
    public void insertInstancesIntoDatabase(){
//        Connection Conn = null;
        try{
            JDBC jdbc = JDBC.getDefaultConnection();

//            Class.forName("virtuoso.jdbc4.Driver");
//            Conn = DriverManager.getConnection("jdbc:virtuoso://localhost:1111/UID=dba/PWD=dba");
            for(int classIndex=0; classIndex<12; classIndex++){
                ClassInstancesDesc desc = classInstancesArray.get(classIndex);

                //Insert ontology class into database
                String strInsertClassStmt = "INSERT INTO " + Constants.ONTOLOGY_CLASS_TABLENAME + " VALUES(" + (classIndex+1)
                        + ", '" + desc.ontologyClass + "')";

                PreparedStatement prepStmt;
                try{
                    prepStmt = jdbc.prepare(strInsertClassStmt);
                    jdbc.executeStatement(prepStmt, new String[0]);
//                    prepStmt.execute();
                    logger.info("Class " + desc.ontologyClass + " is inserted");
                }
                catch (Exception exp){
                    logger.error("Class " + desc.ontologyClass + " cannot be inserted, due to " + exp.getMessage());
                    continue;
                }


                //Insert the instances of the class
                for(URI instanceURI : desc.getInstancesList()){

                    //Here we place 0 to indicate the instance is not currently selected
                    String strInsertInstanceStmt = "INSERT INTO " + Constants.INSTANCE_TABLENAME +
                            " (" + Constants.FIELD_INSTANCE_HASH + ", " + Constants.FIELD_INSTANCE_URI +  ", " +
                            Constants.FIELD_IS_SELECTED +  ", " + Constants.FIELD_CLASS_ID + ")" +
                            " VALUES('" +
                            MD5HashGenerator.getMD5HashCode(instanceURI.toString())+ "', '"+ instanceURI.toString() 
                            + "', "+ 0 + ", " + (classIndex+1) + ")";

                    try{
//                        prepStmt = Conn.prepareStatement(strInsertInstanceStmt);
//                        prepStmt.execute();
                        prepStmt = jdbc.prepare(strInsertInstanceStmt);
                        jdbc.executeStatement(prepStmt, new String[0]);
                        logger.info("Instance named " + instanceURI +" of class " + desc.ontologyClass + " is inserted");
                    }
                    catch (Exception exp){
                        logger.error("Instance named " + instanceURI +" of class " + desc.ontologyClass +
                                " cannot be inserted, due to " + exp.getMessage());
                    }
                }
            }
        }
        catch(Exception exp){
            logger.error("JDBC driver of Virtuoso cannot be loaded, cannot insert into database");            
        }
        finally {
//            try{
//                if((Conn != null) && (!Conn.isClosed()))
//                    Conn.close();
//            }
//            catch(Exception exp){
//
//            }
        }
    }

    public void selectInstancesFromDatabase(){
        try{
            JDBC jdbc = JDBC.getDefaultConnection();
            //Insert ontology class into database
            String strInsertClassStmt = "SELECT * FROM DB.DBA.OntologyClass";
            java.sql.ResultSet rs = jdbc.exec(strInsertClassStmt);
            while(rs.next()){
                System.out.println(rs.getString(2));
            }
            rs.close();

        }
        catch (Exception exp){
            logger.error(exp);
        }

    }

    public List<URI> getSubsetOntologyClassInstances(OntologyClass requiredClass){

        List<URI> allInstancesList = getAllOntologyClassInstances(requiredClass);
        System.out.println("Number of instances " + allInstancesList.size());
//        classInstancesArray.add(allInstancesList);
        classInstancesArray.add(new ClassInstancesDesc(requiredClass, allInstancesList));

        System.out.println("Size of array " + classInstancesArray.size());
        int totalNumberOfInstances = allInstancesList.size();
        int requiredNumberOfInstances = Math.round((BenchmarkConfigReader.percentageOfClassesConsidered/100)
                * totalNumberOfInstances);//The required number of instances to be selected
        return _getRandomIndexedInstancesList(allInstancesList, requiredNumberOfInstances);
    }

    /**
     * Constructs a model out of the instances of all passed class  
     * @param classInstances  A list containing the instances of a specific class
     */
    public void constructOutputGraph(List<URI> classInstances){
        //The model that will contain the output of all construct calls 
        Model outputModel = null, returnedModel = null;
        String ontologyClassInstancesQuery = "";
        QueryEngineHTTP queryExecuter = null;

        for(URI instanceURI : classInstances){
            outputModel = null;

            ontologyClassInstancesQuery = String.format("CONSTRUCT {?s  ?p ?o} FROM <http://dbpedia.org>  WHERE { {?s ?p ?o} " +
                    "FILTER (?s = <%s>)}", instanceURI);

            queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, ontologyClassInstancesQuery);

            try{
                if(outputModel == null){
                    returnedModel = outputModel = queryExecuter.execConstruct();
//                    outputModel = queryExecuter.execConstruct();
                }
                else{
                    returnedModel = queryExecuter.execConstruct();
                    outputModel.add(returnedModel);
                }

                outputModel.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
                logger.info("Instance named " + instanceURI +" has been processed");
                
                _followInstancePath(outputModel);

    //            ResultSet resultSet = queryExecuter.execSelect();
    //
    //            ResultSetRewindable rs =ResultSetFactory.makeRewindable(resultSet);
                //Iterate through returned instances and place them in the output list as URIs
    //            while (resultSet.hasNext()){
    //
    //                QuerySolution sol = resultSet.next();
    //                logger.info(sol.get("?p") + "             " + sol.get("?o"));
    //            }
                
                //TODO this comment must be removed
//                if(BenchmarkConfigReader.includeSubresources){
//                    Model subresourceModel = _getSubresourcesModel(instanceURI);
//                    outputModel.add(subresourceModel);
//                }
                    
                
//                outputModel.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
//                logger.info("Instance named " + instanceURI +" has been processed");
            }
            catch(Exception exp){
                logger.error(exp.getMessage());
            }
            finally{
                queryExecuter.close();
            }
        }
//        try{
//            outputModel.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
//            logger.info("Model is successfully written to output file in format '" + BenchmarkConfigReader.outputFileFormat +"'");
//        }
//        catch(Exception exp){
//            logger.error(exp.getMessage());
//        }

    }

    public void visit(URI instanceURI){
//        RDFNode
        nodesToBeTraversedQueue.add(new ResourceImpl(instanceURI.toString()));
    }

    public void constructOutputGraph(URI instanceURI){
        //The model that will contain the output of all construct calls
        Model outputModel = null, returnedModel = null;
        String ontologyClassInstancesQuery = "";
        QueryEngineHTTP queryExecuter = null;

        {
            outputModel = null;

            ontologyClassInstancesQuery = String.format("CONSTRUCT {?s  ?p ?o} FROM <http://dbpedia.org>  WHERE { {?s ?p ?o} " +
                    "FILTER (?s = <%s>)}", instanceURI);

            queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, ontologyClassInstancesQuery);

            try{
                if(outputModel == null){
                    returnedModel = outputModel = queryExecuter.execConstruct();
//                    outputModel = queryExecuter.execConstruct();
                }
                else{
                    returnedModel = queryExecuter.execConstruct();
                    outputModel.add(returnedModel);
                }

//                outputModel.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
                _writeModelToFile(outputModel);
                logger.info("Instance named " + instanceURI +" has been processed");

                //Now the node is visited and we should mark it as visited in the database
                JDBC jdbc = JDBC.getDefaultConnection();
                String setNodeSelected = "UPDATE " + Constants.INSTANCE_TABLENAME + " SET "+  Constants.FIELD_IS_SELECTED +" = 1 WHERE "
                        + Constants.FIELD_INSTANCE_URI + " = ?" ;
                PreparedStatement prepStmt = jdbc.prepare(setNodeSelected);
                jdbc.executeStatement(prepStmt, new String[]{instanceURI.toString()});

                _followInstancePath(outputModel);

    //            ResultSet resultSet = queryExecuter.execSelect();
    //
    //            ResultSetRewindable rs =ResultSetFactory.makeRewindable(resultSet);
                //Iterate through returned instances and place them in the output list as URIs
    //            while (resultSet.hasNext()){
    //
    //                QuerySolution sol = resultSet.next();
    //                logger.info(sol.get("?p") + "             " + sol.get("?o"));
    //            }

                //TODO this comment must be removed
//                if(BenchmarkConfigReader.includeSubresources){
//                    Model subresourceModel = _getSubresourcesModel(instanceURI);
//                    outputModel.add(subresourceModel);
//                }


//                outputModel.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
//                logger.info("Instance named " + instanceURI +" has been processed");
            }
            catch(Exception exp){
                logger.error(exp.getMessage());
            }
            finally{
                queryExecuter.close();
            }
        }
//        try{
//            outputModel.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
//            logger.info("Model is successfully written to output file in format '" + BenchmarkConfigReader.outputFileFormat +"'");
//        }
//        catch(Exception exp){
//            logger.error(exp.getMessage());
//        }

    }

    private Model _getSubresourcesModel(URI instanceURI) {
        Model subresourcesModel = null;
        String subresourcesQuery = "";
        QueryEngineHTTP queryExecuter = null;

        subresourcesQuery = "CONSTRUCT {?subresource  ?p ?o} FROM <http://dbpedia.org>  WHERE { <" + instanceURI +"> " +
                    "?somep ?subresource . ?subresource ?p  ?o . FILTER (?subresource LIKE <" + instanceURI + "/%>)}";

        /*
          " where { " + this.subjectSPARULpattern + " ?somep ?subresource . ?subresource ?p  ?o . FILTER (?subresource LIKE <"
        + subject + "/%>)}";
		sparul += where ;
        */

        queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, subresourcesQuery);

        try{
                subresourcesModel = queryExecuter.execConstruct();
        }
        catch(Exception exp){
            logger.error("Subresources for instance " + instanceURI + " cannot be extracted due to " + exp.getMessage());
        }

        return subresourcesModel;
    }

    private void _insertTriplesFromGraph(Graph graph){
        JDBC jdbc = JDBC.getDefaultConnection();
        String query = "SPARQL INSERT IN GRAPH <http://dbpedia.org>"+
        "{"+
        "<http://dbpedia.org/virtrdf-data-formats#default-testteype> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/schemas/virtrdf#QuadMapFormat>."+
        "}";
//        java.sql.ResultSet result = jdbc.exec("sparql select ?s ?p ?o FROM <http://dbpedia.org> where { ?s ?p ?o.}");
        java.sql.ResultSet result = jdbc.exec(query);
        
    }

    private void _writeGraphTriplesToFile(Graph graph){
        try{
            FileOutputStream fs = new FileOutputStream(BenchmarkConfigReader.outputFileName);
            TurtleWriter turtleWriter=new TurtleWriter(fs);
            turtleWriter.startRDF();
            ExtendedIterator<Triple> iterator = graph.find(null);

            N3TurtleJenaWriter writer = new N3TurtleJenaWriter();
            N3JenaWriter wr = new N3JenaWriter();
            while(iterator.hasNext()){
                Triple tr = iterator.next();

            }
            turtleWriter.endRDF();
        }
        catch(Exception exp){
            logger.error("Unable to write triples into file due to " + exp.getMessage());
        }

    }

    /**
     * Selects a specific set of instance out of all the available instance space at random
     * @param allInstancesList  A list containing all instances
     * @param requiredNumber    The number of instances that should be selected
     * @return  A list containing  the randomly selected URIs
     */
    private List<URI> _getRandomIndexedInstancesList(List<URI> allInstancesList, int requiredNumber ){

        if(requiredNumber >= allInstancesList.size())
            return allInstancesList;

        List<URI> selectedInstancesList = new ArrayList<URI>();

        Random generator = new Random();
        int currentRandomNumber = 0;

        //We generate a random number which coincides with the list boundaries, and then remove that element from the large
        //list and place it in the output list
        while((currentRandomNumber < requiredNumber) && (!allInstancesList.isEmpty())){

            int selectedClassNumber = generator.nextInt(allInstancesList.size());
//            System.out.println("The selected number is " + selectedClassNumber);
            URI selectedInstanceURI = allInstancesList.remove(selectedClassNumber);
            selectedInstancesList.add(selectedInstanceURI);
            currentRandomNumber++;
        }
        return selectedInstancesList;
    }

    /**
     * Follows the statements in the passed model till the end of the series to force the Model to remain consistent
     * @param requiredModel
     */
    private void _followInstancePath(Model requiredModel){
        uriLinkedTriples.clear();

        StmtIterator iteratorForStatements = requiredModel.listStatements();

        Model outputModel = requiredModel;
//        outputModel.removeAll();
        int counter = 0;
        while(iteratorForStatements.hasNext()){
            Statement currentStatement = iteratorForStatements.next();

//            org.dbpedia.helper.Triple t = new org.dbpedia.helper.Triple();
//
//            System.out.println("The MD5 hash of current statement is " + MD5HashGenerator.getMD5HashCode(currentStatement));
//            System.out.println("The MD5 hash of current statement for second time is " + MD5HashGenerator.getMD5HashCode(currentStatement));
            if(currentStatement.getObject().isResource())
            {
//                _followURIPath(currentStatement.getObject());
                RDFNode rdf = currentStatement.getObject();
                nodesToBeTraversedQueue.add(currentStatement.getObject());
//                breadthFirstSearch();
//                    dfs(currentStatement);
//            System.out.println("BFS ended");
//            System.exit(0);
            }
//            for(Statement stmt: uriLinkedTriples)
//                System.out.println("FROM ARRAY " + uriLinkedTriples.size());
//            counter++;
//            if(counter == 20)
//                break;
        }
        System.out.println("_followInstancePath ended");
    }

    /**
     * Gets the statements related to the passed resource
     * @param subjectURI    The URI of the required resource which will play the role of the subject 
     */
    private void _followURIPath(RDFNode subjectURI){
        //Get all statements related to the passed subject 
        String subjectRelatedTriplesQuery = String.format("CONSTRUCT {?s  ?p ?o} FROM <http://dbpedia.org>  WHERE { {?s ?p ?o} " +
                    "FILTER (?s = <%s>)}", subjectURI);

        QueryEngineHTTP queryExecuter = null;

        queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, subjectRelatedTriplesQuery);

        Model testModel =  queryExecuter.execConstruct();

        //Iterate through the returned triples, and whenever another URI is encountered as object, we will call this
        // function recursively   
        StmtIterator iteratorForStatements = testModel.listStatements();
        while(iteratorForStatements.hasNext()){
            Statement currentStatement = iteratorForStatements.next();

            //If the object of the statement is a URI, then we should add the statement and recursively iterate through
            //the other statements related to it.
            String MD5Hashcode = MD5HashGenerator.getMD5HashCode(currentStatement);
            if((currentStatement.getObject().isResource()) && !(uriLinkedTriples.containsKey(MD5Hashcode))){
                uriLinkedTriples.put(MD5Hashcode, currentStatement);
                System.out.println("INSIDE RECURSION " + uriLinkedTriples.size());
                _followURIPath(currentStatement.getObject());
            }
            else if (!currentStatement.getObject().isResource())//If the object of the statement is not a URI, we should just add the statement
                uriLinkedTriples.put(MD5Hashcode, currentStatement);
            else
                System.out.println("Already visited");
        }

    }

//    public void dfs(Statement stmt)
//    {
//        //DFS uses Stack data structure
//        Stack s=new Stack();
//        s.push(this.rootNode);
//        rootNode.visited=true;
//        printNode(rootNode);
//        while(!s.isEmpty())
//        {
//            Node n=(Node)s.peek();
//            Node child = getUnvisitedChildNode(n);
//            if(child!=null)
//            {
//                child.visited=true;
//                printNode(child);
//                s.push(child);
//            }
//            else
//            {
//                s.pop();
//            }
//        }
//        //Clear visited property of nodes
//        clearNodes();
//    }

    public void breadthFirstSearch()
    {

//        printNode(this.rootNode);
//        visitedNodes.put(MD5HashGenerator.getMD5HashCode(root.toString()), root);
        while(!nodesToBeTraversedQueue.isEmpty())
        {
            RDFNode node = nodesToBeTraversedQueue.remove();
            logger.info("Node " + node.toString() + " has been eliminated");

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

//            while((child=getUnvisitedChildNode(node))!=null)
//            {
//                child.visited=true;
//                printNode(child);
//                q.add(child);
//            }


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
                nodesToBeTraversedQueue.add(objectNode);
//                visitedNodes.put(MD5HashGenerator.getMD5HashCode(objectNode.toString()), objectNode);
                System.out.println("Node " + objectNode.toString() + " is inserted to queue, and queue size = " +
                        nodesToBeTraversedQueue.size());
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
//            if(modelToBeWritten.size()>20)
            {
                modelToBeWritten.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
                logger.info("Model is successfully written to file");
            }
        }
        catch (Exception exp){
            logger.error("Model cannot be written to file due to " + exp.getMessage(), exp);
        }
    }


}
