package org.benchmark.jenatdb;

import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Dec 8, 2010
 * Time: 8:48:15 PM
 * Loads RDF data to a JenaTDB triplestore
 */
public class JenaTDBDataLoader {
    private static Logger logger = Logger.getLogger(JenaTDBDataLoader.class);
    private static final long TOTAL_NUMBER_OF_TRIPLES = 153000000;

    public static void loadData(){

        try {
//            File inputFile = new File("D:/benchmark_10.nt");
            File inputFile = new File(BenchmarkConfigReader.jenaTDBInputTriplesFile);
            FileReader fReader = new FileReader(inputFile);
            LineNumberReader lnReader = new LineNumberReader(fReader);

            File tempFile = new File(inputFile.getParent() + "/jenatdbtemp.nt");
//            GraphTDB graph = new GraphTDB();
//            DatasetGraphTDB dataset = TDBFactory.createDatasetGraph("/opt/jena_tdb/TDB-0.8.7/dbpedia");
            DatasetGraphTDB dataset = TDBFactory.createDatasetGraph(BenchmarkConfigReader.jenaTDBDatasetGraph);
//            DatasetGraphTDB dataset = TDBFactory.createDatasetGraph("d:/output");

//            dataset.
//            Dataset dataset = TDBFactory.createDataset("/opt/jena_tdb/TDB-0.8.7/dbpedia") ;

            TDBLoader loader = new TDBLoader();
            long numberOfTriplesLeft = TOTAL_NUMBER_OF_TRIPLES;
            long numberOfTripleLoaded = 0;

            
            while(readPartOfInputFile(lnReader, tempFile)){
                try{

                    long startTime = System.nanoTime();
                    logger.info("before upload");
//                    TDBLoader.load
//                    loader.loadDataset(dataset, new FileInputStream(tempFile));
                    BulkLoader.loadDataset(dataset, new FileInputStream(tempFile), true);
                    logger.info("after upload");
                    double endTime = (System.nanoTime() - startTime)/1000000.0;

                    numberOfTriplesLeft -= 10000;
                    numberOfTripleLoaded += 10000;
                    //calculate estimated time to finish in minutes
                    long estimatedTimeLeft = (long)((numberOfTriplesLeft * (endTime /1000))/10000);

                    estimatedTimeLeft = (long)(estimatedTimeLeft/60);
                    logger.info(numberOfTripleLoaded +" triples are written successfully into JenaTDB, and it took " + endTime +
                            " milliSeconds,  estimated minutes left = " + estimatedTimeLeft + " minutes");
                    tempFile = new File(inputFile.getParent() + "/temp.nt");
                }
                catch (Exception exp){
                    logger.error("Failed to upload data, du to " + exp.getMessage(), exp);
                }
            }

            lnReader.close();
            tempFile.delete();
//            loader.loadDataset(dataset, "");
            logger.info("Data loaded successfully");

        }
         catch (java.io.IOException exp) {
           logger.error("Failed to load data, due to " + exp.getMessage(), exp);

        }
    }

    /**
     * Reads 10000 Triples from the input file and places them into the output file, in order to facilitate inserting
     * the contents of large input N-TRIPLES file into Sesame, as very large files block the insertion process
     * @param lnReader    The N-TRIPLES input file
     * @param outFile   The N-TRIPLES output file, that will contain 1000 triples from the input file
     * @return  Whether the process is performed successfully or not
     */
    private static boolean readPartOfInputFile(LineNumberReader lnReader, File outFile){
        try{
            FileWriter outWriter = new FileWriter(outFile);
            String triple = "";
            int numberOfLines = 0;
            while ((triple = lnReader.readLine()) != null){
                numberOfLines++;
                if(numberOfLines > 10000)
                    break;
                outWriter.write(triple + "\r\n");
            }
            outWriter.close();
            if(numberOfLines >0)
                return true;
            else
                return false;
        }
        catch (Exception exp){
            logger.error("Temporary N-TRIPLES file cannot be created, due to" + exp.getMessage(), exp);
            return true;
        }

    }


}
