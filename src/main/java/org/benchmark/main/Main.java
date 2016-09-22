package org.benchmark.main;

import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.querying.QueryExecutor;
import org.benchmark.querying.QueryLoader;
import org.benchmark.querying.SPARQLQuery;
import org.benchmark.sesame.SesameQueryExecutor;
import org.benchmark.sesame.SesameQueryExecutorUsingJena;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Sep 29, 2010
 * Time: 10:20:33 AM
 * Main class
 */
public class Main {
    private static Logger logger = Logger.getLogger(Main.class);
    private static double[] executionTimesForQueries = new double[25];

    public static void main2(String[] args){

        for(int i = 0; i<executionTimesForQueries.length; i++)
            executionTimesForQueries[i] = 0;
        BenchmarkConfigReader.readBenchmarkConfiguration();
        QueryExecutor exec = new SesameQueryExecutorUsingJena();
        exec.executeQuery(null);


    }

    public static void main(String[] args){
        for(int i = 0; i<executionTimesForQueries.length; i++)
            executionTimesForQueries[i] = 0;
        BenchmarkConfigReader.readBenchmarkConfiguration();

//        ExtendedDatasetGenerator.generateExtendedDataset();
//        DataGenerator.generateData();

//        SesameDataLoader.loadData();
//        JenaTDBDataLoader.loadData();
//        logger.info("Triples = " + SesameDataLoader.countNumberOfTriples());
//        SesameQueryExecutor sesameExec = new SesameQueryExecutor();
//        sesameExec.executeQuery("DESCRIBE ?s FROM <http://dbpedia.org> WHERE {?s ?p ?o} LIMIT 100");
//        generateData();
//        ClusterProcessor.sortQueries();
//        ClusterProcessor.makeIDs(false, false, "\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");
//        ClusterProcessor.makeQueryIDsWithFrequencies("");



//        ClusterProcessor.loadClustersToDatabase();
//        ExtendedDatasetGenerator.generateExtendedDataset();
//        ExtendedDatasetGenerator.countResourceTriplesInDataset();
//        JenaTDBDataLoader.loadData();
//        ClustersTableUtils.updateClustersTable();
//        SesameDataLoader.loadData();

//        BigOWLIMQueryExecutor.executeTestQuery();
//        SesameDataLoader.loadDataThroughHTTP();
//        generateData();
//        Statistics.calculateOutDegree();


//        ClusterProcessor.loadClustersToDatabase();
//        calculateQueriesSimilarities();
//        ClusterProcessor.makeIDs(true, true,"");

//        VirtuosoDataLoader.loadData();
//        SesameDataLoader.loadDataThroughHTTP();

//       JenaTDBDataLoader.loadData();
//        JenaTDBQueryExecutor.executeTestQuery();

//        JenaTDBQueryExecutor e= new JenaTDBQueryExecutor();

//        e.executeQuery("SELECT ?var1 WHERE { <http://dbpedia.org/resource/Stuttgart> <http://dbpedia.org/property/aprHi_percent_C2_percent_B0c> ?var1 . }");
//        SesameDataLoader.loadData();

//        ExtendedDatasetGenerator.countTriplesinExtendedDataset();



        logger.info("STARTING EVALUATION OF RUNNING TIME OF EACH QUERY ON SESAME");


        ArrayList<SPARQLQuery> arrQueries = QueryLoader.loadQueries();
//        ArrayList<SPARQLQuery> arrQueries = new ArrayList<SPARQLQuery>();
        try{
//            QueryExecutor exec = new SesameQueryExecutorUsingJena();
            QueryExecutor exec = new SesameQueryExecutor();
//              QueryExecutor exec = new SesameQueryExecutorUsingJena();
//            QueryExecutor exec = new VirtuosoQueryExecutor();
//            QueryExecutor exec = new BigOWLIMQueryExecutor();
//            QueryExecutor exec = new BigOWLIMQueryExecutorUsingJena();
//            QueryExecutor exec = new JenaTDBQueryExecutorUsingHTTP();
//            ArrayList<Integer> queryIDsList = ClusterProcessor.keepClusters(1);

            long warmupStartTime = System.currentTimeMillis();
            long warmupEndTime = warmupStartTime + 1000 * 60 * BenchmarkConfigReader.warmupTimePeriod; //We run the warm-up process for the specified number of Minutes

//            ArrayList<Integer> warmupQueryIDsList = getRandomQueryIDsForWarmingUp(queryIDsList);
//            HashMap<Integer, String> hmWarmupQueries = VirtuosoQueryLoader.getQueriesForIDs(queryIDsList,"\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");

            logger.info("=========================WARM-UP PHASE STARTED======================");
            int warmupQueryNumber = 0;
            while(System.currentTimeMillis()<warmupEndTime){
                //warm-up process
               
//                Iterator warmupQueryIterator = hmWarmupQueries.entrySet().iterator();

//                while(warmupQueryIterator.hasNext()){
                for(SPARQLQuery sparqlrQuery : arrQueries){
                    if(sparqlrQuery.query.compareTo("") == 0){
                        continue;
                    }
                    try{
                        exec.executeWarmUpQuery(sparqlrQuery);
                    }
                    catch (Exception exp){
                        logger.info("Inside catch for timer");
                    }
                }
                warmupQueryNumber ++;
                //end of warm-up process

            }
            logger.info("Difference = " + (System.currentTimeMillis() - warmupStartTime)/1000);
            logger.info("=========================WARM-UP PHASE ENDED======================");


//            HashMap<Integer, String> hmQueries = VirtuosoQueryLoader.getQueriesForIDs(queryIDsList,"\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");

            long startTime;
            double endTime;
            int queryNumber = 1;


            //This file will contain the final average running times of all queries along with QMpH
            File queryExecutionOutputFile = new File(BenchmarkConfigReader.queryExecutionTimeFile);
            FileWriter queryExecutionOutWriter = new FileWriter(queryExecutionOutputFile);

            //This file will contain the running time of each query in every single run
            File queryRunningTimeOutputFile = new File(BenchmarkConfigReader.runningTimesFile);
            FileWriter queryRunningTimeOutWriter = new FileWriter(queryRunningTimeOutputFile);

            //            FileWriter outWriterQueryTimes = new FileWriter("/opt/akswbenchmark/queries/jenatdb/queriesAvg.txt");
//            queryExecutionOutWriter.write("Execution times against " + BenchmarkConfigReader.sparqlEndpoint + "\n");
            queryExecutionOutWriter.write("QueryID\tTotal Execution Time\tAvg Execution Time\tNumber of Queries per Hour\n");

//            Map sortedQueriesMap = new TreeMap<Integer, String>(hmQueries);

//            Iterator queryIterator = sortedQueriesMap.entrySet().iterator();

//            for(String _query : arrQueries){
//
//                double executionTime = VirtuosoQueryExecutor.executeQuery(_query);
//                logger.info("Query # " + queryNumber + " has taken " + executionTime + " Micro-seconds");
//                queryNumber++;
//            }

            int QueryNumber = 1;

//            QueryExecutor exec = new VirtuosoQueryExecutor();

//            while(queryIterator.hasNext()){
//                Map.Entry<Integer, String> pair = (Map.Entry<Integer, String>)queryIterator.next();
//
//                double executionTime = exec.executeQuery(pair.getValue());
//
//                logger.info("Query # " + queryNumber + ", Query ID = " +pair.getKey() + " has taken " + executionTime + " Micro-seconds");
//                queryNumber++;
//                queryExecutionOutWriter.write(pair.getKey() + "\t" + executionTime + "\n");
//                Thread.sleep(2000);
//            }

            //Preparing the headers for running times file
            for(int i = 0; i < arrQueries.size(); i++)
                queryRunningTimeOutWriter.write("Query"+i+"\t");
            queryRunningTimeOutWriter.write("\n/////////////////////////////////////////////////////////////////////////////\n");


            int queryMixesPerHour = 0;

            long actualExecutionStartTime = System.currentTimeMillis();
            long actualExecutionEndTime = actualExecutionStartTime + 1000 * 60 * BenchmarkConfigReader.actualRunningTimePeriod; //We run the actual process for the specified number of Minutes

            logger.info("=========================ACTUAL RUN PHASE STARTED=========================");



            while(System.currentTimeMillis()<actualExecutionEndTime){
                queryNumber = 0;
                for(SPARQLQuery sparqlrQuery : arrQueries){

                    //We want to execlude the queries containing regualr expressions
//                    if(sparqlrQuery.features.contains("regex")){
//                        queryNumber ++;
//                        continue;
//                    }

                    //query is empty so it should be skipped
                    if(sparqlrQuery.query.compareTo("") == 0){
                        queryNumber++;
                        queryRunningTimeOutWriter.write("-1\t");
                        continue;
                    }


                    double executionTime = exec.executeQuery(sparqlrQuery);

                    queryRunningTimeOutWriter.write(((int)executionTime) + "\t");

                    logger.info("Query # " + queryNumber + ",  has taken " + executionTime + " Micro-seconds");
//                    outWriterQueryTimes.write(queryNumber + "\t" + executionTime + "\n");
                    executionTimesForQueries[queryNumber] += executionTime;
                    queryNumber++;
    //                Thread.sleep(2000);

                }
                queryMixesPerHour ++;
                logger.info("Query mixes per hour = " + queryMixesPerHour);
                queryRunningTimeOutWriter.write("\n");

            }
//            outWriterQueryTimes.flush();
//            outWriterQueryTimes.close();
            
            logger.info("=========================ACTUAL RUN PHASE ENDED=========================");

//            queryMixesPerHour = queryMixesPerHour;//we multiply by 3 as we run this only for 20 minutes, and we want it to be per hour

            //calculate the average execution time for each query individually;
            for(queryNumber = 0; queryNumber<executionTimesForQueries.length; queryNumber++){
                double totalExecutionTimeForQuery = executionTimesForQueries[queryNumber];

                executionTimesForQueries[queryNumber] = executionTimesForQueries[queryNumber]/queryMixesPerHour;

                //since the execution time of each query is measures in microseconds, we must convert it into hours,
                //as we want the number of queries per hour
                double numberOfSingleQueriesPerHour = -1;
                if(executionTimesForQueries[queryNumber] > 0)//to avoid division by zero in case of negelecting regex queries
                    numberOfSingleQueriesPerHour = ((long)60 * 60 * 1000000) / (double)executionTimesForQueries[queryNumber];
                //queryExecutionOutWriter.write("QueryID\tTotal Execution Time\tAvg Execution Time\tNumber of Queries per Hour\n");
                queryExecutionOutWriter.write((queryNumber) + "\t" + totalExecutionTimeForQuery + "\t" +
                        executionTimesForQueries[queryNumber] + "\t" + numberOfSingleQueriesPerHour + "\n");
            }

            queryExecutionOutWriter.write("/////////////////////////////////////////////////////////////////////////////\n");
            queryExecutionOutWriter.write("QMpH = " + queryMixesPerHour);
            queryExecutionOutWriter.flush();
            queryExecutionOutWriter.close();

            queryRunningTimeOutWriter.flush();
            queryRunningTimeOutWriter.close();

            logger.info("Execution times are written successfully");
            System.exit(0);
        }
        catch(Exception exp){
            logger.error("Problem with the execution of queries, program cannot continue due to " + exp, exp);
            System.exit(1);
        }


        
//        VirtuosoQueryExecutor.executeQuery("SELECT * WHERE {?s ?p ?o}");
//        VirtuosoQueryExecutor.executeQuery(arrQueries.get(0));
//        double EndTime = (System.nanoTime() - StartTime)/1000000000.0;
//        System.out.print("It took " + EndTime);
    }

    

    /**
     * Generates random IDs for queries other than those selected for testing. Those queries are only used for warming
     * the system up
     * @param selectedQueryIDs  The list containing the IDs of the queries used for testing, so those IDs are execluded
     * @return  A list of IDs for queries used for warming the system up
     */
    private static ArrayList<Integer> getRandomQueryIDsForWarmingUp(ArrayList<Integer> selectedQueryIDs){
        Random generator = new Random();
        int max = selectedQueryIDs.get(selectedQueryIDs.size()-1);
        ArrayList<Integer> outputList = new ArrayList<Integer>();
        while(outputList.size()<20){
            int randomID = generator.nextInt(max);
            if(!selectedQueryIDs.contains(randomID))
                outputList.add(randomID);
        }
        
        return outputList;
    }

}