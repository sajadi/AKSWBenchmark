package org.benchmark.main;

import org.apache.log4j.Logger;
import org.benchmark.clustering.ClusterProcessor;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.jenatdb.JenaTDBQueryExecutor;
import org.benchmark.querying.QueryExecutor;
import org.benchmark.querying.QueryLoader;
import org.benchmark.querying.SPARQLQuery;
import org.benchmark.virtuoso.VirtuosoQueryLoader;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Feb 1, 2011
 * Time: 11:16:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewMain {

    private static Logger logger = Logger.getLogger(NewMain.class);
    private static double[] executionTimesForQueries = new double[25];

    public static void main(String[] args){
        for(int i = 0; i<executionTimesForQueries.length; i++)
            executionTimesForQueries[i] = 0;
        BenchmarkConfigReader.readBenchmarkConfiguration();
        logger.info("=========================NEW METHODOLOGY=========================");
        logger.info("STARTING EVALUATION OF RUNNING TIME OF EACH QUERY ON VIRTUOSO TRIPLESTORE");
        ArrayList<SPARQLQuery> arrQueries = QueryLoader.loadQueries();



        try{
//            QueryExecutor exec = new SesameQueryExecutor();
//            QueryExecutor exec = new VirtuosoQueryExecutor();
            QueryExecutor exec = new JenaTDBQueryExecutor();
            ArrayList<Integer> queryIDsList = ClusterProcessor.keepClusters(150);

            long warmupStartTime = System.currentTimeMillis();
            long warmupEndTime = warmupStartTime + 1000 * 60 * BenchmarkConfigReader.warmupTimePeriod; //We run the warm-up process for the specified number of Minutes


            HashMap<Integer, String> hmWarmupQueries = VirtuosoQueryLoader.getQueriesForIDs(queryIDsList,"\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");

            logger.info("=========================WARM-UP PHASE STARTED======================");
            for(int i=0; i<300; i++){
                //warm-up process
                int j=0;
                for(SPARQLQuery sparqlrQuery : arrQueries){

                    //query is empty so it should be skipped
                    logger.info("========================= Query # =" + j + " ======================");
                    if(sparqlrQuery.query.compareTo("") == 0){
                        continue;
                    }
                    try{
                        exec.executeWarmUpQuery(sparqlrQuery);
                    }
                    catch (Exception exp){
                        logger.info("Inside catch fr timer");
                    }
                    j++;
                }

                //end of warm-up process

            }
            logger.info("Difference = " + (System.currentTimeMillis() - warmupStartTime)/1000);
            logger.info("=========================WARM-UP PHASE ENDED======================");


            HashMap<Integer, String> hmQueries = VirtuosoQueryLoader.getQueriesForIDs(queryIDsList,"\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");

            long startTime;
            double endTime;
            int queryNumber = 1;

            //Preparing header info
            File queryExecutionOutputFile = new File(BenchmarkConfigReader.queryExecutionTimeFile);
            FileWriter outWriter = new FileWriter(queryExecutionOutputFile);

            outWriter.write("QueryID\tTotal Execution Time\tAvg Execution Time\tNumber of Queries per Hour\n");

            Map sortedQueriesMap = new TreeMap<Integer, String>(hmQueries);

            Iterator queryIterator = sortedQueriesMap.entrySet().iterator();

            int QueryNumber = 1;

            int queryMixesPerHour = 0;

            long actualExecutionStartTime = System.currentTimeMillis();
            long actualExecutionEndTime = actualExecutionStartTime + 1000 * 60 * BenchmarkConfigReader.actualRunningTimePeriod; //We run the actual process for the specified number of Minutes

            logger.info("=========================ACTUAL RUN PHASE STARTED=========================");

            for(int i=0; i<600; i++){
                queryNumber = 0;
                for(SPARQLQuery sparqlrQuery : arrQueries){

                    //query is empty so it should be skipped
                    if(sparqlrQuery.query.compareTo("") == 0){
                        queryNumber++;
                        continue;
                    }


                    double executionTime = exec.executeQuery(sparqlrQuery);

                    logger.info("Query # " + queryNumber + ",  has taken " + executionTime + " Micro-seconds");
                    executionTimesForQueries[queryNumber] += executionTime;
                    queryNumber++;

                }
                queryMixesPerHour ++;
            }

            logger.info("=========================ACTUAL RUN PHASE ENDED=========================");
            startTime = System.nanoTime();
            //calculate the average execution time for each query individually;
            for(queryNumber = 0; queryNumber<executionTimesForQueries.length; queryNumber++){
                double totalExecutionTimeForQuery = executionTimesForQueries[queryNumber];

                executionTimesForQueries[queryNumber] = executionTimesForQueries[queryNumber]/queryMixesPerHour;

                //since the execution time of each query is measures in microseconds, we must convert it into hours,
                //as we want the number of queries per hour
                double numberOfSingleQueriesPerHour = -1;
                if(executionTimesForQueries[queryNumber] > 0)//to avoid division by zero in case of negelecting regex queries
                    numberOfSingleQueriesPerHour = ((long)60 * 60 * 1000000) / (double)executionTimesForQueries[queryNumber];
                //outWriter.write("QueryID\tTotal Execution Time\tAvg Execution Time\tNumber of Queries per Hour\n");
                outWriter.write((queryNumber) + "\t" + totalExecutionTimeForQuery + "\t" +
                        executionTimesForQueries[queryNumber] + "\t" + numberOfSingleQueriesPerHour + "\n");
            }

            endTime = (System.nanoTime() - startTime)/1000.0;
            endTime = endTime / 1000000.0;

            outWriter.write("/////////////////////////////////////////////////////////////////////////////\n");
            outWriter.write("600 QMpH has taken " + endTime + " seconds");
            outWriter.flush();
            outWriter.close();
            logger.info("Execution times are written successfully");
        }
        catch(Exception exp){
            logger.error("Problem with the execution of queries, program cannot continue due to " + exp, exp);
            System.exit(1);
        }

    }
}
