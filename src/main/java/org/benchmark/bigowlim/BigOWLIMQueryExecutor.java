package org.benchmark.bigowlim;

import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.querying.QueryExecutor;
import org.benchmark.querying.SPARQLQuery;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;

import javax.swing.*;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Mar 13, 2011
 * Time: 12:39:35 PM
 * Executes the queries against a BigOWLIM SPARQL end-point with underlying BigOWLIM repository.
 */
public class BigOWLIMQueryExecutor implements QueryExecutor {
    private static Logger logger = Logger.getLogger(BigOWLIMQueryExecutor.class);
    private Timer executionTimer; //To avoid block of a call

    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
    private static String bigOWLIMServer = BenchmarkConfigReader.bigOWLIMServerAddress;
    private static String repositoryID = BenchmarkConfigReader.bigOWLIMRepositoryID;
    private static Repository myRepository;
    private static RepositoryConnection con;
    private static FileWriter errorWriter;
    private static int showResults = 0;

    static{
        try{
            myRepository = new HTTPRepository(bigOWLIMServer, repositoryID);
            myRepository.initialize();
            con = myRepository.getConnection();
        }
        catch(Exception exp)
        {}
    }

    private static <T> T timedCall(FutureTask<T> task, long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        THREAD_POOL.execute(task);
        return task.get(timeout, timeUnit);
    }

    public static void executeTestQuery(){
        try{

            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 100");
            TupleQueryResult rs = tupleQuery.evaluate();
             while(rs.hasNext()){
                 BindingSet varSet = rs.next();
                 logger.info("FROM repository " + myRepository.toString());
                 logger.info( varSet.getValue("s") + "\t" +  varSet.getValue("p") + "\t" +  varSet.getValue("o"));
             }
        }
        catch(Exception exp){
            logger.error(exp);

        }
    }

    public double executeQuery(SPARQLQuery sparqlQuery){
          String query = sparqlQuery.prepareQueryForExecution();
        try {
            String queryLowerCase = query.toLowerCase();

            long startTime;
            double endTime;

            try {
                query = removeFromPart(queryLowerCase);
                query = removeCommasBetweenVariables(query);
                query = addPrefixes(query);

                logger.info("*****************************BIGOWLIM EXECUTE_QUERY *****************************");

                QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(con, query);
                FutureTask<Double> queryExecutionTask = new FutureTask<Double>(executorWithLimit);
                try{
                    return timedCall(queryExecutionTask, BenchmarkConfigReader.queryExecutionTimeLimit, TimeUnit.MINUTES);
                }
                catch (Exception exp){
                    queryExecutionTask.cancel(true);
                    errorWriter = new FileWriter(BenchmarkConfigReader.errorFile, true);
                    errorWriter.write(query + "\n");
                    errorWriter.close();

                    logger.info("Failed to execute query, and it was forced to terminate after the allowed period has passed" + query);
                    return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000;
                }


           }
           catch(Exception exp){
               logger.error("Query = " + query + " was not executed successfully", exp);
           }
           finally {
           }
        }
        catch (Exception exp) {
            logger.error("Query = " + query + " was not executed successfully", exp);
           return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000;
        }
       return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000;
    }

    private double _executeQuery(String query)  {
        try{

            String queryLowerCase = query.toLowerCase();
            long startTime;
            double endTime;
            startTime = System.nanoTime();
//            _query = "select DISTINCT ?var1 where {?var1 ?p ?o} LIMIT 1000";
            if(queryLowerCase.contains("select")) {

                TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult rs = tupleQuery.evaluate();
                 /*while(rs.hasNext()){
                     BindingSet varSet = rs.next();
                     Value node = varSet.getValue("var1");
                     logger.info("node = " + node.stringValue());
                 }*/
            }
            else if(queryLowerCase.contains("ask")) {
                BooleanQuery booleanQuery =  con.prepareBooleanQuery(QueryLanguage.SPARQL, query);
                booleanQuery.evaluate();
            }
            else {
                GraphQuery graphQuery = con.prepareGraphQuery(QueryLanguage.SPARQL, query);
                graphQuery.evaluate();
            }

    //                g.evaluate();
            //TupleQueryResult result = tupleQuery.evaluate();
            endTime = (System.nanoTime() - startTime)/1000.0;
            logger.info("Query executed successfully");
             return endTime;
        }
        catch(Exception exp){
            logger.error("Query = " + query + " was not executed successfully", exp);
            return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000;
        }

    }

    /**
     * The BigOWLIM triplestore, working under Sesame-Workbench, does not accept a FROM-Clause in the SPARQL _query as we must define a Repository
     * so we should remove it, if it exists
     * @param query The _query in which the FROM-Clause may exist
     * @return  The _query after removing the FROM-Clause, so it's ready for execution on BigOWLIM
     */
    private String removeFromPart(String query){
        int fromPosition = query.toLowerCase().indexOf("from ");

        //FROM-Clause is not found, so we will return the _query as it is
        if(fromPosition < 0)
            return query;

        //The end of the FROM-Clause is ">", so find it
        int greaterThanPosition = query.indexOf(">");

        //If greater than is not found we will return the _query as it is
        if(greaterThanPosition < 0)
            return query;

        query = query.substring(0, fromPosition) + query.substring(greaterThanPosition+1);

        return query;
    }

    /**
     * The BigOWLIM triplestore, working under Sesame-Workbench, does not accept a comma-separated variable list in the SELECT-Clause, i.e. the variables
     * must be separated by space not with comma, so we must replace the commas in SELECT-Clause with spaces
     * @param query The _query in which the variables in SELECT-Clause may be comma-separated
     * @return  The _query after replacing the commas in SELECT-Clause with spaces, so it's ready for execution on BigOWLIM
     */
    private String removeCommasBetweenVariables(String query){
        int wherePos =  query.toLowerCase().indexOf("where");
        StringBuffer queryBuffer = new StringBuffer(query);

        int commaPos = query.lastIndexOf(",", wherePos);
        while(commaPos >= 0){
            queryBuffer.setCharAt(commaPos, ' ');
            commaPos = query.lastIndexOf(",", commaPos-1);
        }
        return queryBuffer.toString();
    }

     /**
     * The BigOWLIM triplestore, working under Sesame-Workbench, does not accept some prefixes that are predefined in Virtuoso e.g. dbpprop, so we should
      * add those prefixes to the _query in order to execute it successfully
     * @param query The _query in which we should add the prefixes
     * @return  The _query after adding the required prefixes to it
     * @return  The _query after adding the required prefixes to it
     */
    private String addPrefixes(String query){
         int queryStartPos = -1;
         queryStartPos = query.toLowerCase().indexOf("select");

         if(queryStartPos < 0)
            queryStartPos = query.toLowerCase().indexOf("ask");

         if(queryStartPos < 0)
            queryStartPos = query.toLowerCase().indexOf("construct");

         if(queryStartPos < 0)
            queryStartPos = query.toLowerCase().indexOf("describe");

         if(queryStartPos < 0)
            return query;

         String prefixPart = query.substring(0, queryStartPos);
         if(!prefixPart.contains("dbpprop"))
            query = "PREFIX  dbpprop: <http://dbpedia.org/property/>\n" + query;

         if(!prefixPart.contains("skos"))
            query = "PREFIX  skos: <http://www.w3.org/2004/02/skos/core#>\n"+ query;

         if(!prefixPart.contains("foaf"))
            query = "PREFIX  foaf: <http://xmlns.com/foaf/0.1/>\n" + query;

         if(!prefixPart.contains("rdfs"))
            query = "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + query;

         return query;
     }

    public void executeWarmUpQuery(String query){
        try {

            String queryLowerCase = query.toLowerCase();

            try {
                logger.info("*****************************BIGOWLIM EXECUTE_WARMUP_QUERY*****************************");
                query = removeFromPart(query);
                query = removeCommasBetweenVariables(query);
                query = addPrefixes(query);
                queryLowerCase = query.toLowerCase();


                //I used times call here as in some case, one of the warm-up queries blocks, so we sould terminate it
                //and choose another one
                QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(con, query);
                FutureTask<Double> queryExecutionTask = new FutureTask<Double>(executorWithLimit);
                try{
                    timedCall(queryExecutionTask, 10, TimeUnit.SECONDS);
                    logger.info("Warm-up query executed successfully");
                }
                catch (Exception exp){
                    queryExecutionTask.cancel(true);

                    logger.info("Warm-up query blocks, and it was terminated");
                }
           }
           catch(Exception exp){
               logger.error("Query was not executed successfully");
           }
           finally {
           }
        }
        catch (Exception exp) {
            logger.error("Query = " + query + " was not executed successfully", exp);
        }


    }

    public void executeWarmUpQuery(SPARQLQuery sparqlQuery){

        String query = sparqlQuery.prepareQueryForExecution();
        try {
            String queryLowerCase = query.toLowerCase();

            try {
//                logger.info("Before removeFromPart, quey = " + query);
                query = removeFromPart(queryLowerCase);
//                logger.info("Before removeCommasBetweenVariables, quey = " + query);
                query = removeCommasBetweenVariables(query);
//                logger.info("Before addPrefixes, quey = " + query);
                query = addPrefixes(query);

                logger.info("*****************************BIGOWLIM EXECUTE WARMUP_QUERY MODIFIED*****************************");

                QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(con, query);
                FutureTask<Double> queryExecutionTask = new FutureTask<Double>(executorWithLimit);
                try{
                    timedCall(queryExecutionTask, BenchmarkConfigReader.queryExecutionTimeLimit, TimeUnit.MINUTES);
                }
                catch (Exception exp){
                    queryExecutionTask.cancel(true);

                    logger.info("Failed to execute query, and it was forced to terminate after the allowed period has passed" + query);
                }
           }
           catch(Exception exp){
               logger.error("Query = " + query + " was not executed successfully", exp);
           }
           finally {
           }
        }
        catch (Exception exp) {
            logger.error("Query = " + query + " was not executed successfully", exp);
            return;
        }
       return;

    }


    /**
     * Executes the auxiliary _query and gets a set of possible values for the variable in the original _query
     * @param auxQuery  The auxiliary _query to be executed
     * @param   numberOfRequiredVariableValues  The number of values that will replace tha variable values, as
     * some queries may contain more than one variable that should be replaced
     * @return  A random possible value for the variable
     */
    private ArrayList<String> _getResultsForAuxiliaryQuery(String auxQuery, int numberOfRequiredVariableValues){
        try{
            String bigOWLIMServer = BenchmarkConfigReader.bigOWLIMServerAddress;
            String repositoryID = BenchmarkConfigReader.bigOWLIMRepositoryID;

            Repository myRepository = new HTTPRepository(bigOWLIMServer, repositoryID);
            RepositoryConnection con = myRepository.getConnection();

            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, auxQuery);
            TupleQueryResult rs= tupleQuery.evaluate();

            ArrayList<String> arrValues = new ArrayList<String>();

            //loop until you reach the random position
            String strValue = "";
            while(rs.hasNext()){
                BindingSet varSet = rs.next();
                Value node = varSet.getValue("?var");
                if(node instanceof Resource)
                    strValue = "<" + node.toString() + ">";
                else
                    strValue = "\"" + node.toString() + "\"";

                arrValues.add(strValue);
            }

            //If the auxiliary _query returns no result, we should use another _query that is guaranteed to return result
            if(arrValues.size()<=0){
                arrValues.clear();
                tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL,"SELECT DISTINCT ?var WHERE {  ?var  rdf:type " +
                        "?var1 . FILTER(?var LIKE <http://dbpedia.org/resource/%>)} limit 1000");
                rs = tupleQuery.evaluate();

                while(rs.hasNext()){
                     BindingSet varSet = rs.next();
                    Value node = varSet.getValue("?var");

                    if(node instanceof Resource)
                        strValue = "<" + node.toString() + ">";
                    else
                        strValue = "\"" + node.toString() + "\"";

                    arrValues.add(strValue);
                }
            }

            ArrayList<String> outputValuesList = new ArrayList<String>();
            for(int i = 0; i < numberOfRequiredVariableValues; i++ ){
                //Generate random number to be the required position of the resource or the literal that will be selected
                Random generator = new Random();
                int randomPosition = generator.nextInt(arrValues.size());
                outputValuesList.add(arrValues.remove(randomPosition));
            }

    //        return arrValues.get(randomPosition);
            return outputValuesList;

        }
        catch (Exception exp){
            return null;
        }

    }

/**
     * This class implements Callable interface, which provides the ability to place certain time limit on the execution
     * time of a function, so it must end after a specific time limit
     */
    private class QueryExecutorWithTimeLimit implements Callable<Double> {

        RepositoryConnection bigOWLIMCon;
        String query;

        public QueryExecutorWithTimeLimit(RepositoryConnection bigOWLIMRepositoryCon, String query){
            this.query = query;
        }
        public Double call(){
            try{
                String queryLowerCase = query.toLowerCase();
                long startTime;
                double endTime;

                endTime = startTime = System.nanoTime();
                if(queryLowerCase.contains("select")) {

                    TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    TupleQueryResult rs = tupleQuery.evaluate();
                    /*int numberOfResults = 0;
                    while (rs.hasNext()){
                        rs.next();
                         numberOfResults++;
                    }
                    logger.info("Query = " + query);
                    logger.info("Number of Results = " + numberOfResults);*/
                    /*while(rs.hasNext()){
                        BindingSet varSet = rs.next();
                        Iterator varIterator = varSet.iterator();
                        while(varIterator.hasNext()){
                            System.out.println(varIterator.next().toString() + "\t");
                        }
//                        logger.info( varSet.getValue("") + "\t" +  varSet.getValue("p") + "\t" +  varSet.getValue("o"));
                    }  */

                }
                else if(queryLowerCase.contains("ask")) {
                    BooleanQuery booleanQuery =  con.prepareBooleanQuery(QueryLanguage.SPARQL, query);
                    booleanQuery.evaluate();
                }
                else {
                    GraphQuery graphQuery = con.prepareGraphQuery(QueryLanguage.SPARQL, query);
                    graphQuery.evaluate();
                }

                endTime = (System.nanoTime() - startTime)/1000.0;
                logger.info("Query = " + query);
                logger.info("Query executed successfully");

                return endTime;
            }
            catch(java.lang.IllegalThreadStateException exp){
                logger.error("Query = " + query + " was not executed successfully, original exception is QueryEvaluationException", exp);
                //We should close the connection, and shut the repository down, as several concurrent connections
                //cause and exception of type "java.lang.IllegalThreadStateException" to be thrown

               return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;
            }
            catch(Exception exp){
                logger.error("Query = " + query + " was not executed successfully", exp);

                return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;
            }

        }
    }
}
