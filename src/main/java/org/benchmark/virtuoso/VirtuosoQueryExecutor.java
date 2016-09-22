package org.benchmark.virtuoso;


import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.querying.QueryExecutor;
import org.benchmark.querying.SPARQLQuery;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 3, 2010
 * Time: 11:09:30 AM
 * Executes the queries against a Virtuoso SPARQL end-point.
 */
public class VirtuosoQueryExecutor implements QueryExecutor, Callable<Double> {
    private static Logger logger = Logger.getLogger(VirtuosoQueryExecutor.class);
    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
    private static QueryEngineHTTP queryEngine = null;
//    private static File queryExecutionOutputFile;
//    private static FileWriter outWriter;
    private static FileWriter errorWriter;

    private static <T> T timedCall(FutureTask<T> task, long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        THREAD_POOL.execute(task);
        return task.get(timeout, timeUnit);
    }

    public VirtuosoQueryExecutor(){
//        try{
//            queryExecutionOutputFile = new File("/opt/akswbenchmark/queries/virtuoso/NumberOfResults.txt");
//            outWriter = new FileWriter(queryExecutionOutputFile);
//        }
//        catch(Exception exp){
//
//        }
    }
    
    public double executeQuery(SPARQLQuery sparqlQuery){

//        String query = sparqlQuery._query;
        String query = sparqlQuery.prepareQueryForExecution();

        String queryLowerCase = query.toLowerCase();
        long startTime;
        double endTime;


//        _getResultsForAuxiliaryQuery("SELECT distinct ?var WHERE { ?var ?var4 ?var5} LIMIT 1000");
//          _getResultsForAuxiliaryQuery("SELECT distinct ?var WHERE {  ?var  rdf:type ?var1 . } limit 1000");
//        _getResultsForAuxiliaryQuery("SELECT DISTINCT ?var WHERE {  ?var  rdf:type ?var1 . FILTER(?var " +
//                "LIKE <http://dbpedia.org/resource/%>)} limit 1000");

        //Get the value of the variable that will be replaced in the _query
//        ArrayList<String> queryVariableValue =  _getResultsForAuxiliaryQuery(sparqlQuery.auxiliaryQuery,2);

        //This regular expression is used to extract all variable placeholders fom the _query


        /*Pattern p = Pattern.compile("\\%%.*?%%");
        Matcher matcher = p.matcher(sparqlQuery._query);

        ArrayList<String> arrVariables = new ArrayList<String>();
        while (matcher.find()) {
            System.out.println("Starting & ending index of" + matcher.group()+ ":=" +
            "start=" + matcher.start() + " end = " + matcher.end());
            if(!arrVariables.contains(matcher.group()))
                arrVariables.add(matcher.group());
        }

        logger.info("------------------------------------------------------------------------");
        logger.info(_query);
        logger.info("------------------------------------------------------------------------");

        ArrayList<String> queryVariableValues =  _getResultsForAuxiliaryQuery(sparqlQuery.auxiliaryQuery,arrVariables.size());

        int varIndex = 0;
        while(varIndex < queryVariableValues.size()){
            //Replace each variable in arrVariables with its corresponding value in queryVariableValues   
            _query = _query.replaceAll(arrVariables.get(varIndex), queryVariableValues.get(varIndex));
            varIndex++;
        }

//        _query = _query.replace("$$var$$", queryVariableValue);

        */

        

//        if(queryEngine == null)
//            queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, query);

//        com.hp.hpl.jena.rdql.QueryE q = new com.hp.hpl.jena.rdql.Query(_query);

//        String wherePart = _query.substring(_query.indexOf("{")+1, _query.lastIndexOf("}"));
//        String fQu = "SELECT ?book ?title \n WHERE \n { ?t subject ?book . \n ?t predicate ?title .\n}";
//        com.hp.hpl.jena.rdql.Query q = new com.hp.hpl.jena.rdql.Query(fQu);
//        com.hp.hpl.jena.sparql.lang.rdql q;
//        q = new com.hp.hpl.jena.sparql.lang.rdql(fQu);

        
//        logger.info(q.getTriplePatterns().size());
        logger.info("*****************************VIRTUOSO EXECUTE_QUERY *****************************");

        QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(queryEngine, query);
        FutureTask<Double> queryExecutionTask = new FutureTask<Double>(executorWithLimit);
        try{
            return timedCall(queryExecutionTask, BenchmarkConfigReader.queryExecutionTimeLimit, TimeUnit.MINUTES);
        }
        catch (Exception exp){
            queryExecutionTask.cancel(true);
            logger.info("Failed to execute query " + query);

            try{
                errorWriter = new FileWriter(BenchmarkConfigReader.errorFile, true);
                errorWriter.write(query + "\n");
                errorWriter.close();
            }
            catch(Exception fileExp) {}
            
            //we return the most allowed time limit as the runtime
            return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000;
        }

//        return _executeQuery(queryEngine, query);
    }

    /**
     * Executes a warm up _query.
     * @param query The _query that should be executed
     */
    public void executeWarmUpQuery(String query){
//        String queryLowerCase = _query.toLowerCase();
//        QueryEngineHTTP queryEngine = null;

//        queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, query);
        if(queryEngine == null)
            queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.virtuosoServerAddress, query);
        logger.info("*****************************VIRTUOSO EXECUTE_WARMUP_QUERY *****************************");
        _executeQuery(query);

    }

    public void executeWarmUpQuery(SPARQLQuery sparqlQuery){
        String query = sparqlQuery.prepareQueryForExecution();

        String queryLowerCase = query.toLowerCase();

//        if(queryEngine == null)
//            queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, query);


        logger.info("*****************************VIRTUOSO EXECUTE WARMUP_QUERY *****************************");

        QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(queryEngine, query);
        FutureTask<Double> queryExecutionTask = new FutureTask<Double>(executorWithLimit);
        try{
            timedCall(queryExecutionTask, BenchmarkConfigReader.queryExecutionTimeLimit, TimeUnit.MINUTES);
        }
        catch (Exception exp){
            queryExecutionTask.cancel(true);
            logger.info("Failed to execute query " + query);
        }
    }

    /**
     * Executes a _query against virtuoso
     * We choose to to pass the queryEngine instead of creating it in the function itself to avoid adding more time of its
     * initialization to the actual time used to execute the _query
     * @param query The _query that should be executed.
     * @return  The execution time of the _query.
     */
    private double  _executeQuery( String query) {
        String queryLowerCase = query.toLowerCase();
        /*try{
            if(queryLowerCase.contains("select"))
               queryEngine.execSelect();
            else if(queryLowerCase.contains("ask"))
               queryEngine.execAsk();
            else if(queryLowerCase.contains("describe"))
               queryEngine.execDescribe();
            else
               queryEngine.execConstruct();

            logger.info("Query executed successfully");
        }
        catch (Exception exp){
            logger.error("Query = " + _query + " was not executed successfully", exp);
        }*/
        
        long startTime;
        double endTime;
        try{
             startTime = System.nanoTime();
             if(queryLowerCase.contains("select"))
                queryEngine.execSelect();
             else if(queryLowerCase.contains("ask"))
                queryEngine.execAsk();
             else if(queryLowerCase.contains("describe"))
                queryEngine.execDescribe();
             else
                queryEngine.execConstruct();

             endTime = (System.nanoTime() - startTime)/1000.0;
//             endTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime);
             logger.info("Query executed successfully");
             return endTime;
         }
         catch (Exception exp){
             logger.error("Query = " + query + " was not executed successfully", exp);
             
             //we return the most allowed time limit as the runtime
            return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000;
         }
    }

    /**
     * Executes the auxiliary _query and gets a set of possible values for the variable in the original _query
     * @param auxQuery  The auxiliary _query to be executed
     * @param   numberOfRequiredVariableValues  The number of values that will replace tha variable values, as
     * some queries may contain more than one variable that should be replaced
     * @return  A random possible value for the variable
     */
    private ArrayList<String> _getResultsForAuxiliaryQuery(String auxQuery, int numberOfRequiredVariableValues){
        QueryEngineHTTP queryEngine = null;
        queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, auxQuery);
        ResultSet rs = queryEngine.execSelect();

        ArrayList<String> arrValues = new ArrayList<String>();

        //loop until you reach the random position
        String strValue = "";
        while(rs.hasNext()){
            RDFNode node = rs.nextSolution().get("?var");
            if(node.isResource())
                strValue = "<" + node.toString() + ">";
            else
                strValue = "\"" + node.toString() + "\"";

            arrValues.add(strValue);
        }

        /*
        //If the auxiliary _query returns no result, we should use another _query that is guaranteed to return result
        if(arrValues.size()<=0){
            arrValues.clear();
            queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint,
                    "SELECT DISTINCT ?var WHERE {  ?var  rdf:type ?var1 . FILTER(?var LIKE <http://dbpedia.org/resource/%>)} limit 1000");
            rs = queryEngine.execSelect();

            while(rs.hasNext()){
                RDFNode node = rs.nextSolution().get("?var");
                if(node.isResource())
                    strValue = "<" + node.toString() + ">";
                else
                    strValue = "\"" + node.toString() + "\"";

                arrValues.add(strValue);
            }
        }
        */
        
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

    public Double call() throws Exception {
        return 0.0;
    }

    /**
     * This class implements Callable interface, which provides the ability to place certain time limit on the execution
     * time of a function, so it must end after a specific time limit 
     */
    private class QueryExecutorWithTimeLimit implements Callable<Double>{
        private QueryEngineHTTP virtuosoQueryEngine;
        private QueryExecution execu;
        String query;
        public QueryExecutorWithTimeLimit(QueryEngineHTTP queryEngine, String query){
            try{
            this.query = query;
//            virtuosoQueryEngine = queryEngine;
//            virtuosoQueryEngine.abort();
//            virtuosoQueryEngine.close();

            }
            catch(Exception exp){
                logger.error("QUERY " + query + " cannot be parsed");
            }
//            virtuosoQueryEngine = (QueryEngineHTTP)com.hp.hpl.jena.query.QueryExecutionFactory.create(query);
        }
        public Double call(){
            String queryLowerCase = query.toLowerCase();

             virtuosoQueryEngine = (QueryEngineHTTP)com.hp.hpl.jena.query.QueryExecutionFactory.sparqlService(
                    BenchmarkConfigReader.virtuosoServerAddress, query);

            long startTime;
            double endTime;
            try{
                ResultSet rsReturnedResults = null;
                 startTime = System.nanoTime();
                 if(queryLowerCase.contains("select")){
                     int numberOfResults = 0;
                    rsReturnedResults = virtuosoQueryEngine.execSelect();
                      /*while(rsReturnedResults.hasNext()){
                          rsReturnedResults.next();
                          numberOfResults++;

                      }*/
                     logger.info("Query = " + query);
                     logger.info("Number of Results = " + numberOfResults);

                 }
                 else if(queryLowerCase.contains("ask"))
                    virtuosoQueryEngine.execAsk();
                 else if(queryLowerCase.contains("describe"))
                    virtuosoQueryEngine.execDescribe();
                 else
                    virtuosoQueryEngine.execConstruct();

                 endTime = (System.nanoTime() - startTime)/1000.0;

                logger.info(query);


                 /*int numberOfResults = 0;
                if(rsReturnedResults!=null){
                     while(rsReturnedResults.hasNext()){
                         QuerySolution sol = rsReturnedResults.nextSolution();
                         logger.info(sol.get(rsReturnedResults.getResultVars().get(0)));
                         numberOfResults++;
                     }

                    try{
                    while(rsReturnedResults.nextSolution()!=null)
                        numberOfResults++;
                    }
                    catch(NoSuchElementException exp){
                        numberOfResults++;
                    }
                }
                outWriter.write(numberOfResults + "\r\n");
                logger.info(numberOfResults);*/

                 virtuosoQueryEngine.abort();
                 virtuosoQueryEngine.close();


                 logger.info("Query executed successfully against EndPoint " + BenchmarkConfigReader.virtuosoServerAddress );
                 return endTime;
             }
             catch (Exception exp){
                 logger.error("Query = " + query + " was not executed successfully", exp);
                 virtuosoQueryEngine.abort();
                 virtuosoQueryEngine.close();
                 //we return the most allowed time limit as the runtime
                return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;
             }
        }
    }
}
