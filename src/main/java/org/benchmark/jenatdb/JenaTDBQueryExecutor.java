package org.benchmark.jenatdb;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.tdb.TDBFactory;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.querying.QueryExecutor;
import org.benchmark.querying.SPARQLQuery;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 26, 2010
 * Time: 6:33:42 PM
 * Executes the queries against a JenaTDB
 */
public class JenaTDBQueryExecutor implements QueryExecutor {
    private static Logger logger = Logger.getLogger(JenaTDBQueryExecutor.class);
    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
    private static File queryExecutionOutputFile;
    private static FileWriter outWriter;
    private static FileWriter errorWriter;
    private static int showResults = 0;

    private static <T> T timedCall(FutureTask<T> task, long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        THREAD_POOL.execute(task);
        return task.get(timeout, timeUnit);
    }

    public JenaTDBQueryExecutor(){
        try{
            queryExecutionOutputFile = new File("/opt/akswbenchmark/queries/jenatdb/NumberOfResults.txt");
            outWriter = new FileWriter(queryExecutionOutputFile);
        }
        catch(Exception exp){

        }
    }

    public static void executeTestQuery(){
        Dataset dataset = TDBFactory.createDataset(BenchmarkConfigReader.jenaTDBDatasetGraph) ;
        QueryExecution execFactory = QueryExecutionFactory .create("SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 100", dataset);

        ResultSet rs =  execFactory.execSelect();
        while(rs.hasNext()){
            QuerySolution sol = rs.nextSolution();
            logger.info("FROM graph" + BenchmarkConfigReader.jenaTDBDatasetGraph);
            logger.info(sol.get("?s") + "\t" + sol.get("?p") + "\t" + sol.get("?o"));
        }

    }

    public double executeQuery(SPARQLQuery sparqlQuery){
//        String query = sparqlQuery._query;
        String query = sparqlQuery.prepareQueryForExecution();
        String queryLowerCase = query.toLowerCase();
        long startTime;
        double endTime;
        startTime = System.nanoTime();

        query = removeFromPart(query);
        query = removeCommasBetweenVariables(query);
        query = addPrefixes(query);

        try{
//            com.hp.hpl.jena.graph._query.Query graphQuery = new com.hp.hpl.jena.graph._query.Query();
//            SPARQLParser parser = new SPARQLParser();
//            ParsedQuery parsed = parser.parseQuery(queryLowerCase, null);

//               _query = "select DISTINCT ?s where{?s ?p ?o} LIMIT 10000";

//            Query graphQuery = QueryFactory.create(queryLowerCase);
//            String gg = graphQuery.getQueryPattern().toString();
//                int size = graphQuery.getPattern().size();
//            Dataset dataset = TDBFactory.createDataset("/opt/jena_tdb/TDB-0.8.7/dbpedia") ;
            Dataset dataset = TDBFactory.createDataset(BenchmarkConfigReader.jenaTDBDatasetGraph) ;
//            query = "SELECT ?s ?p ?o where {?s ?p ?o} LIMIT 100";
            QueryExecution execFactory = QueryExecutionFactory.create(query, dataset);


//            com.hp.hpl.jena._query.Query ee;
//                ee.

//            com.hp.hpl.jena.rdql.Query queryw;
            ////////////////////////////////////////////////////////////////////////////
            logger.info("*****************************JenaTDB EXECUTE_QUERY *****************************");
            /* Old style code
            return _executeQuery(execFactory, query);
            */
             QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(execFactory, query);
             FutureTask<Double> queryExecutionTask = new FutureTask<Double>(executorWithLimit);
             try{
                 return timedCall(queryExecutionTask, BenchmarkConfigReader.queryExecutionTimeLimit, TimeUnit.MINUTES);
             }
             catch (Exception exp){
                 queryExecutionTask.cancel(true);
                 logger.info("Failed to execute query, and it was forced to terminate after the allowed period has passed" + query);

                 try{
                    errorWriter = new FileWriter(BenchmarkConfigReader.errorFile, true);
                    errorWriter.write(query + "\n");
                    errorWriter.close();
                 }
                 catch(Exception fileExp) {}
                 
                 return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;
             }
        }
        catch (Exception exp){
            logger.info("Query " + query + " cannot be executed against Jena TDB");
            return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;
        }
        ////////////////////////////////////////////////////////////////////////////
//        execFactory.execSelect();

//        endTime = (System.nanoTime() - startTime)/1000000000000.0;
//        logger.info("Executed............................, and it took " + endTime + " Micro-sec");

//        QueryEngineHTTP queryEngine = null;
//        queryEngine = new QueryEngineHTTP("http://localhost:2020/myservice.html", _query);
//        queryEngine.execSelect();
//
//        TDBFactory.

//        return 0;
    }



    public void executeWarmUpQuery(String query){
        try{
            String queryLowerCase = query.toLowerCase();
            Dataset dataset = TDBFactory.createDataset(BenchmarkConfigReader.jenaTDBDatasetGraph);

            query = removeFromPart(query);
            query = removeCommasBetweenVariables(query);
            query = addPrefixes(query);

            QueryExecution execFactory = QueryExecutionFactory .create(query, dataset);
            _executeQuery(execFactory, query);
        }
        catch (Exception exp){
            logger.info("Query " + query + " cannot be executed against Jena TDB");
        }
         /*try{
             logger.info("*****************************JenaTDB EXECUTE_WARMUP_QUERY *****************************");
             if(queryLowerCase.contains("select"))
                execFactory.execSelect();
             else if(queryLowerCase.contains("ask"))
                execFactory.execAsk();
             else if(queryLowerCase.contains("describe"))
                execFactory.execDescribe();
             else
                execFactory.execConstruct();

             logger.info("Query executed successfully");
         }
         catch (Exception exp){
             logger.error("Query = " + _query + " was not executed successfully", exp);
         } */

    }

    public void executeWarmUpQuery(SPARQLQuery sparqlQuery){
        String query = sparqlQuery.prepareQueryForExecution();
        String queryLowerCase = query.toLowerCase();

        query = removeFromPart(query);
        query = removeCommasBetweenVariables(query);
        query = addPrefixes(query);

        try{

            Dataset dataset = TDBFactory.createDataset(BenchmarkConfigReader.jenaTDBDatasetGraph) ;
            QueryExecution execFactory = QueryExecutionFactory.create(query, dataset);

            ////////////////////////////////////////////////////////////////////////////
            logger.info("*****************************JenaTDB EXECUTE WARMUP_QUERY*****************************");

             QueryExecutorWithTimeLimit executorWithLimit = new QueryExecutorWithTimeLimit(execFactory, query);
             FutureTask<Double> queryExecutionTask = new FutureTask<Double>(executorWithLimit);
             try{
                 timedCall(queryExecutionTask, BenchmarkConfigReader.queryExecutionTimeLimit, TimeUnit.MINUTES);
             }
             catch (Exception exp){
                 queryExecutionTask.cancel(true);
                 logger.info("Failed to execute query, and it was forced to terminate after the allowed period has passed" + query);
             }
        }
        catch (Exception exp){
            logger.info("Query " + query + " cannot be executed against Jena TDB");
        }
    }

    /**
     * Executes a _query against Jena TDB
     * We choose to to pass the queryExecuter instead of creating it in the function itself to avoid adding more time of its
     * initialization to the actual time used to execute the _query
     * @param queryExecutor   The engine that is used to execute the _query.
     * @param query The _query that should be executed.
     * @return  The execution time of the _query.
     */
    private double _executeQuery(QueryExecution queryExecutor, String query) {
        String queryLowerCase = query.toLowerCase();
        long startTime;
        double endTime;
        try{
             startTime = System.nanoTime();
             if(queryLowerCase.contains("select")){
//                 queryExecuter.execSelect();
//                logger.info("QUERY = " + query);
                ResultSet rs =  queryExecutor.execSelect();
                 /*while(rs.hasNext())
                     logger.info(rs.nextSolution().get("?s"));*/
             }
             else if(queryLowerCase.contains("ask"))
                queryExecutor.execAsk();
             else if(queryLowerCase.contains("describe"))
                queryExecutor.execDescribe();
             else
                queryExecutor.execConstruct();

             endTime = (System.nanoTime() - startTime)/1000.0;
             logger.info("Warm-up query executed successfully");
             return endTime;
         }
         catch (Exception exp){
             logger.error("Query = " + query + " was not executed successfully", exp);
             return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;//if the query is not executed successfully, we should consider it as taking too much time
         }
    }

    /**
     * The Jena-TDB triplestore does not accept a FROM-Clause in the SPARQL _query as we must define a Repository
     * so we should remove it, if it exists
     * @param query The _query in which the FROM-Clause may exist
     * @return  The _query after removing the FROM-Clause, so it's ready for execution on Jena-TDB
     */
    private String removeFromPart(String query){
        int fromPosition = query.toLowerCase().indexOf("from");

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
     * The Jena-TDB triplestore does not accept a comma-separated variable list in the SELECT-Clause, i.e. the variables
     * must be separated by space not with comma, so we must replace the commas in SELECT-Clause with spaces
     * @param query The _query in which the variables in SELECT-Clause may be comma-separated
     * @return  The _query after replacing the commas in SELECT-Clause with spaces, so it's ready for execution on Jena-TDB
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
     * The Jena-TDB triplestore does not accept some prefixes that are predefined in Virtuoso e.g. dbpprop, so we should
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

         /*String prefixes = "PREFIX  dbpprop: <http://dbpedia.org/property/>\n"
                 + "PREFIX  skos: <http://www.w3.org/2004/02/skos/core#>\n"
                 + "PREFIX  foaf: <http://xmlns.com/foaf/0.1/>\n"
                 + "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n";
         _query = prefixes + _query;*/
         return query;
     }

    /**
     * This class implements Callable interface, which provides the ability to place certain time limit on the execution
     * time of a function, so it must end after a specific time limit
     */
    private class QueryExecutorWithTimeLimit implements Callable<Double> {
        QueryExecution queryExecutor;
        String query;

        public QueryExecutorWithTimeLimit(QueryExecution queryExecutor, String query){
//            this.queryExecutor = queryExecutor;
            this.query = query;
        }

        public Double call(){
            String queryLowerCase = query.toLowerCase();

            Dataset dataset = TDBFactory.createDataset(BenchmarkConfigReader.jenaTDBDatasetGraph) ;
            this.queryExecutor = QueryExecutionFactory.create(query, dataset);
            logger.info("Connection opened");

            long startTime;
            double endTime;
            try{
                 startTime = System.nanoTime();
                 ResultSet rsReturnedResults = null;
                 if(queryLowerCase.contains("select")){
//                     logger.info("Query = " + query);
                     rsReturnedResults =  this.queryExecutor.execSelect();
                     /*int numberOfResults = 0;
                     while(rsReturnedResults.hasNext()){
                        rsReturnedResults.next();
                         numberOfResults++;
                     }

                     logger.info("Query = " + query);
                     logger.info("Number of Results = " + numberOfResults);*/
                 }
                 else if(queryLowerCase.contains("ask"))
                    this.queryExecutor.execAsk();
                 else if(queryLowerCase.contains("describe"))
                    this.queryExecutor.execDescribe();
                 else
                    this.queryExecutor.execConstruct();



//                logger.info("QUERY = " + query);

                /*int numberOfResults = 1;

//                    if(showResults % 7 == 0){
                        while(rsReturnedResults.hasNext()){
                            numberOfResults++;

    //                        if(numberOfResults > 100000)
    //                            break;
                        }*/
//                logger.info("Number of results = " + numberOfResults);
//                    }
//                    showResults++ ;
//                     while(rsReturnedResults.hasNext()){
//                         QuerySolution sol = rsReturnedResults.nextSolution();
//                         logger.info(sol.get(rsReturnedResults.getResultVars().get(0)));
//                         numberOfResults++;
//                     }

                    /*try{
                        while(rsReturnedResults.hasNext())
                        {
//                            QuerySolution sol = rsReturnedResults.nextSolution();
                            numberOfResults++;
                            break;
                        }
                    }
                    catch(NoSuchElementException exp){
                        logger.info("NoSuchElementException is thrown");
                        numberOfResults++;
                    }*/

//                outWriter.write(numberOfResults + "\r\n");
//                if(numberOfResults > 0)
//                logger.info("# Of Results = " + numberOfResults);

//                this.queryExecutor.abort();
                endTime = (System.nanoTime() - startTime)/1000.0;
                logger.info("Query = " + query);
                this.queryExecutor.close();

                logger.info("Connection closed");

                 logger.info("Query executed successfully");
                 return endTime;
             }
             catch (Exception exp){
                 logger.error("Query = " + query + " was not executed successfully", exp);
                 this.queryExecutor.abort();
                 this.queryExecutor.close();
                 logger.info("Connection closed");
                 return BenchmarkConfigReader.queryExecutionTimeLimit * 60 * 1000000.0;//if the query is not executed successfully, we should consider it as taking too much time
             }
        }
    }

}
