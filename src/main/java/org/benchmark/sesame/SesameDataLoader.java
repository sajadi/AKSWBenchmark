package org.benchmark.sesame;

import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.openrdf.OpenRDFException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.repository.manager.LocalRepositoryManager;
import org.openrdf.rio.RDFFormat;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.util.Set;


/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 11, 2010
 * Time: 10:04:21 AM
 * Loads RDF data to a Sesame triplestore
 */
public class SesameDataLoader {

    private static Logger logger = Logger.getLogger(SesameDataLoader.class);
    private static final long TOTAL_NUMBER_OF_TRIPLES = 275000000;

    public static void loadData(){

        try {
//            File inputFile = new File("D:/benchmark_10.nt");
            File inputFile = new File(BenchmarkConfigReader.sesameInputTriplesFile);
            FileReader fReader = new FileReader(inputFile);
            LineNumberReader lnReader = new LineNumberReader(fReader);

            File tempFile = new File(inputFile.getParent() + "/sesametemp.nt");

    //        File inputFile = new File("/opt/akswbenchmark/output/benchmark_10.nt");
            String baseURI = "http://example.org/example/local";
//            String sesameServerAddress = "http://localhost:9999/sesame";
//            String sesameServer = "http://139.18.2.96:8080/sesame";
            String sesameServer = BenchmarkConfigReader.sesameServerAddress;
            String repositoryID = BenchmarkConfigReader.sesameRepositoryID;

//            Repository myRepository = new HTTPRepository(sesameServer, repositoryID);

            LocalRepositoryManager dbpediaRepositoryMgr = new LocalRepositoryManager(
                    new File("/usr/share/tomcat6/.aduna/openrdf-sesame/"));


            logger.info(LocalRepositoryManager.REPOSITORIES_DIR);
            try{
                dbpediaRepositoryMgr.initialize();
                logger.info("Rep = " + dbpediaRepositoryMgr.toString());
                Set<String> repIDs = dbpediaRepositoryMgr.getRepositoryIDs();
                logger.info("SIZE = " + repIDs);
                for(String str : repIDs)
                logger.info("ID = " + str);
            }
            catch(Exception exp){
                logger.error(exp);

            }

             Repository myRepository = dbpediaRepositoryMgr.getRepository(BenchmarkConfigReader.sesameRepositoryID);
//            myRepository.initialize();
            long numberOfTriplesLeft = TOTAL_NUMBER_OF_TRIPLES;
            long numberOfTriplesUploaded = 0;
//
            logger.info("Start of upload");

            RepositoryConnection con = myRepository.getConnection();

            logger.info("Uploading from file " + BenchmarkConfigReader.sesameInputTriplesFile + " started");
            //There is a method con.add(tempFile, null, RDFFormat.NTRIPLES) for uploading data but it is too slow
            //This is the parser we want to speed up
//            RDFParser ntriplesParser = Rio.createParser(RDFFormat.NTRIPLES, con.getValueFactory());


//            NTriplesParser ntriplesParser = new NTriplesParser(con.getValueFactory());
//            ntriplesParser.setVerifyData(false);
//            ntriplesParser.setStopAtFirstError(false);
//            ntriplesParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
//
//            RDFInserter ntriplesInserter = new RDFInserter(con);
//
//            ntriplesParser.setRDFHandler(ntriplesInserter);
//            con.setAutoCommit(true);
            try {
                while(readPartOfInputFile(lnReader, tempFile)){
                    try{
                        logger.info("Loading data from " + BenchmarkConfigReader.sesameInputTriplesFile);
                        long startTime = System.nanoTime();

                        con.add(tempFile, null, RDFFormat.NTRIPLES);
                        
                        con.commit();
//                        ntriplesParser.parse(new FileInputStream(tempFile),tempFile.toURI().toString());
                        double endTime = (System.nanoTime() - startTime)/1000000.0;

                        numberOfTriplesLeft -= 10000;
                        numberOfTriplesUploaded += 10000;

                        //calculate estimated time to finish in minutes
                        long estimatedTimeLeft = (long)((numberOfTriplesLeft * (endTime /1000))/10000);
                        estimatedTimeLeft = (long)(estimatedTimeLeft/60);

                        logger.info(numberOfTriplesUploaded + " triples are written successfully into Sesame, and it took " + endTime +
                                " milli-Seconds, estimated minutes left = " + estimatedTimeLeft + " minutes");
                        tempFile = new File(inputFile.getParent() + "/sesametemp.nt");
                    }
                    catch (Exception exp){
                        logger.error("10K triples are not loaded due to " + exp.getMessage(), exp);
                    }
                }

                logger.info("Data loaded successfully");
//              URL url = new URL("http://example.org/example/remote");
//              con.add(url, url.toString(), RDFFormat.NTRIPLES);
           }
           finally {
                lnReader.close();
                tempFile.delete();
                con.close();
           }
        }
        catch (OpenRDFException exp) {
            logger.error("Failed to load data, due to " + exp.getMessage(), exp);
        }
        catch (java.io.IOException exp) {
           logger.error("Failed to load data, due to " + exp.getMessage(), exp);

        }

    }

    public static void loadDataThroughHTTP(){

        try {
//            File inputFile = new File("D:/benchmark_10.nt");
            File inputFile = new File(BenchmarkConfigReader.sesameInputTriplesFile);
            FileReader fReader = new FileReader(inputFile);
            LineNumberReader lnReader = new LineNumberReader(fReader);

            File tempFile = new File(inputFile.getParent() + "/sesametemp.nt");

    //        File inputFile = new File("/opt/akswbenchmark/output/benchmark_10.nt");
            String baseURI = "http://example.org/example/local";
//            String sesameServerAddress = "http://localhost:9999/sesame";
//            String sesameServer = "http://139.18.2.96:8080/sesame";
            String sesameServer = BenchmarkConfigReader.sesameServerAddress;
            String repositoryID = BenchmarkConfigReader.sesameRepositoryID;

            Repository myRepository = new HTTPRepository(sesameServer, repositoryID);

//            LocalRepositoryManager dbpediaRepositoryMgr = new LocalRepositoryManager(
//                    new File("/usr/share/tomcat6/.aduna/openrdf-sesame/"));


//            logger.info(LocalRepositoryManager.REPOSITORIES_DIR);
//            try{
//                dbpediaRepositoryMgr.initialize();
//                logger.info("Rep = " + dbpediaRepositoryMgr.toString());
//                Set<String> repIDs = dbpediaRepositoryMgr.getRepositoryIDs();
//                logger.info("SIZE = " + repIDs);
//                for(String str : repIDs)
//                logger.info("ID = " + str);
//            }
//            catch(Exception exp){
//                logger.error(exp);
//
//            }

//            Repository myRepository = dbpediaRepositoryMgr.getRepository("dbpedia200.org");
            myRepository.initialize();
            long numberOfTriplesLeft = TOTAL_NUMBER_OF_TRIPLES;
            long numberOfTriplesUploaded = 0;
//
            RepositoryConnection con = myRepository.getConnection();



            //There is a method con.add(tempFile, null, RDFFormat.NTRIPLES) for uploading data but it is too slow
            //This is the parser we want to speed up
//            RDFParser ntriplesParser = Rio.createParser(RDFFormat.NTRIPLES, con.getValueFactory());


//            NTriplesParser ntriplesParser = new NTriplesParser(con.getValueFactory());
//            ntriplesParser.setVerifyData(false);
//            ntriplesParser.setStopAtFirstError(false);
//            ntriplesParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
//
//            RDFInserter ntriplesInserter = new RDFInserter(con);
//
//            ntriplesParser.setRDFHandler(ntriplesInserter);
            try {
                while(readPartOfInputFile(lnReader, tempFile)){
                    try{
                        logger.info("Loading data through HTTP from file " + BenchmarkConfigReader.sesameInputTriplesFile
                            +", to repository "+ BenchmarkConfigReader.sesameRepositoryID);
                        long startTime = System.nanoTime();

                        con.add(tempFile, null, RDFFormat.NTRIPLES);
                        con.commit();
//                        ntriplesParser.parse(new FileInputStream(tempFile),tempFile.toURI().toString());
                        double endTime = (System.nanoTime() - startTime)/1000000.0;

                        numberOfTriplesLeft -= 10000;
                        numberOfTriplesUploaded += 10000;

                        //calculate estimated time to finish in minutes
                        long estimatedTimeLeft = (long)((numberOfTriplesLeft * (endTime /1000))/10000);
                        estimatedTimeLeft = (long)(estimatedTimeLeft/60);

                        logger.info(numberOfTriplesUploaded + " triples are written successfully into Sesame, and it took " + endTime +
                                " milli-Seconds, estimated minutes left = " + estimatedTimeLeft + " minutes");
                        tempFile = new File(inputFile.getParent() + "/sesametemp.nt");
                    }
                    catch (Exception exp){
                        logger.error("10K triples are not loaded due to " + exp.getMessage(), exp);
                    }
                }

                logger.info("Data loaded successfully");
//              URL url = new URL("http://example.org/example/remote");
//              con.add(url, url.toString(), RDFFormat.NTRIPLES);
           }
           finally {
                lnReader.close();
                tempFile.delete();
                con.close();
           }
        }
        catch (OpenRDFException exp) {
            logger.error("Failed to load data, due to " + exp.getMessage(), exp);
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

    public static long countNumberOfTriples(){
        try {
//            File inputFile = new File("D:/benchmark_10.nt");
            File inputFile = new File(BenchmarkConfigReader.sesameInputTriplesFile);

            String sesameServer = BenchmarkConfigReader.sesameServerAddress;
            String repositoryID = BenchmarkConfigReader.sesameRepositoryID;

            Repository myRepository = new HTTPRepository(sesameServer, repositoryID);

            myRepository.initialize();

            RepositoryConnection con = myRepository.getConnection();
            
            /*RepositoryResult<Resource> AvailableGraphs = con.getContextIDs();
            while(AvailableGraphs.hasNext())
            {
                logger.info("before next");
                logger.info(AvailableGraphs.next().stringValue());
                logger.info("after next");
            }*/
            long TripleCount = con.size(myRepository.getValueFactory().createURI("http://dbpedia.org"));
            return TripleCount;
        }
        catch (OpenRDFException exp) {
            logger.error("Failed to count number of triples, due to " + exp.getMessage(), exp);
            return 0;
        }
        catch (Exception exp) {
           logger.error("Failed to count number of triples, due to " + exp.getMessage(), exp);
           return 0;
        }
    }

}
