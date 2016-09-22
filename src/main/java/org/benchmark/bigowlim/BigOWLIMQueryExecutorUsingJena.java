package org.benchmark.bigowlim;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.querying.QueryExecutor;
import org.benchmark.querying.SPARQLQuery;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Mar 20, 2011
 * Time: 9:25:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class BigOWLIMQueryExecutorUsingJena  implements QueryExecutor {
    private static Logger logger = Logger.getLogger(BigOWLIMQueryExecutorUsingJena.class);
    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
    private static QueryEngineHTTP queryEngine = null;
    private static File queryExecutionOutputFile;
    private static FileWriter outWriter;
    private static FileWriter errorWriter;

    private static <T> T timedCall(FutureTask<T> task, long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        THREAD_POOL.execute(task);
        return task.get(timeout, timeUnit);
    }


    public double executeQuery(SPARQLQuery sparqlQuery){

        String query = sparqlQuery.prepareQueryForExecution();

//        String queryLowerCase = query.toLowerCase();


        logger.info("*****************************BIGOWLIM USING JENA EXECUTE_QUERY *****************************");

        QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(query);
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

    }

    /**
     * Executes a warm up _query.
     * @param query The _query that should be executed
     */
    public void executeWarmUpQuery(String query){
        if(queryEngine == null)
            queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, query);
        logger.info("*****************************BIGOWLIM USING JENA EXECUTE_WARMUP_QUERY *****************************");
        _executeQuery(query);

    }

    public void executeWarmUpQuery(SPARQLQuery sparqlQuery){
        String query = sparqlQuery.prepareQueryForExecution();

//        String queryLowerCase = query.toLowerCase();

        if(queryEngine == null)
            queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, query);


        logger.info("*****************************BIGOWLIM USING JENA EXECUTE WARMUP_QUERY *****************************");

        QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(query);
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
     * Executes a _query against BigOWLIM
     * We choose to to pass the queryEngine instead of creating it in the function itself to avoid adding more time of its
     * initialization to the actual time used to execute the _query
     * @param query The _query that should be executed.
     * @return  The execution time of the _query.
     */
    private double  _executeQuery( String query) {
        String queryLowerCase = query.toLowerCase();

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


        ArrayList<String> outputValuesList = new ArrayList<String>();
        for(int i = 0; i < numberOfRequiredVariableValues; i++ ){
            //Generate random number to be the required position of the resource or the literal that will be selected
            Random generator = new Random();
            int randomPosition = generator.nextInt(arrValues.size());
            outputValuesList.add(arrValues.remove(randomPosition));
        }

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
        private QueryEngineHTTP BigOWLIMQueryEngine;
        private QueryExecution execu;
        String query;

        public QueryExecutorWithTimeLimit(String query){
            try{
            this.query = query;

            }
            catch(Exception exp){
                logger.error("QUERY " + query + " cannot be parsed");
            }
        }
        public Double call(){
            String queryLowerCase = query.toLowerCase();

            long startTime;
            double endTime = BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;
            try{
//                queryLowerCase = "select * where { ?var1 a <http://dbpedia.org/ontology/Organisation> . ?var2 <http://dbpedia.org/ontology/foundationPlace> <http://dbpedia.org/resource/London> . ?var4 <http://dbpedia.org/ontology/developer> ?var2 . ?var4 a <http://dbpedia.org/ontology/TelevisionShow> . } LIMIT 1000";

                BigOWLIMQueryEngine = new QueryEngineHTTP(BenchmarkConfigReader.bigOWLIMServerAddress, query);
                //BigOWLIMQueryEngine.addParam("queryLn", "SPARQL");

                ResultSet rsReturnedResults = null;
                 startTime = System.nanoTime();
                 if(queryLowerCase.contains("select"))
                 {
                     try{
                         rsReturnedResults = BigOWLIMQueryEngine.execSelect();
                         /*int numberOfResults = 0;
                         while(rsReturnedResults.hasNext()){
                            rsReturnedResults.next();
                             numberOfResults++;
                         }

                         logger.info("Query = " + query);
                        logger.info("Number of Results = " + numberOfResults);*/
                     }
                     //if the query does not return a result, then an exception will be thrown

                     catch(com.hp.hpl.jena.sparql.resultset.ResultSetException exp){
                         logger.info("FAILED");
                     }
                 }
                 else if(queryLowerCase.contains("ask"))
                    BigOWLIMQueryEngine.execAsk();
                 else if(queryLowerCase.contains("describe"))
                    BigOWLIMQueryEngine.execDescribe();
                 else
                    BigOWLIMQueryEngine.execConstruct();



                endTime = (System.nanoTime() - startTime)/1000.0;
                logger.info("Query = " + query);
                BigOWLIMQueryEngine.abort();
                BigOWLIMQueryEngine.close();

                logger.info("Connection closed");

                logger.info("Query executed successfully against EndPoint " + BenchmarkConfigReader.bigOWLIMServerAddress );

                return endTime;
             }
             catch (Exception exp){
                 logger.error("Query = " + query + " was not executed successfully", exp);

                 logger.error(BenchmarkConfigReader.bigOWLIMServerAddress + "----" + BigOWLIMQueryEngine.toString());
                 try{
                     errorWriter = new FileWriter(BenchmarkConfigReader.errorFile, true);
                     errorWriter.write(query + "\n");
                     errorWriter.close();
                 }
                 catch(Exception ex){

                 }
                 BigOWLIMQueryEngine.abort();
                 BigOWLIMQueryEngine.close();
                 logger.info("Connection closed");

                 //we return the most allowed time limit as the runtime
                return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;
             }
        }
    }
}
