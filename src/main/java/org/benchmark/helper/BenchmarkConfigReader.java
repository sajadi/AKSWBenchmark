package org.benchmark.helper;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Aug 12, 2010
 * Time: 4:29:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class BenchmarkConfigReader {
    private static Logger logger = Logger.getLogger(BenchmarkConfigReader.class);
    private static final String benchmarkConfigFile = "./benchmark.xml";

    private static DocumentBuilderFactory dbFactory;
    private static DocumentBuilder dBuilder;
    private static Document doc;

    //Tag names that are use in live.config file
    private static final String PERCENTAGE_OF_DATA_REQUIRED_TAGNAME = "percentageOfDataRequired";
    private static final String PERCENTAGE_OF_CLASSES_CONSIDERED_TAGNAME = "percentageOfClassesConsidered";
    
    private static final String STORES_TAGNAME = "stores";
    private static final String STORE_TAGNAME = "store";
    private static final String DSN_TAGNAME = "dsn";
    private static final String USERNAME_TAGNAME = "username";
    private static final String PASSWORD_TAGNAME = "password";
    private static final String TYPE_ATTRIBUTENAME = "type";
    private static final String OUTPUT_FILE_TAGNAME = "outputFile";
    private static final String INCLUDE_SUBRESOURCES_TAGNAME = "includeSubresources";
    private static final String SPARQL_ENDPOINT_TAGNAME = "sparqlEndpoint";
    private static final String NUMBER_OF_TRIPLES_TAGNAME = "numberOfTriples";
    private static final String EXTRACTION_METHOD_TAGNAME = "extractionMethod";
    private static final String CLASSES_INPUT_FILENAME_TAGENAME = "classesInputFileName";
    private static final String TRIPLES_INPUT_FOLDERNAME_TAGENAME = "triplesInputFolderName";
    private static final String DECOMPRESS_FILES_TAGENAME = "decompressFiles";
    private static final String QUERY_LOG_FOLDER_TAGENAME = "queryLogFolder";
    private static final String SORTED_QUERIES_OUTPUT_FILE_TAGENAME = "sortedQueriesOutputFile";
    private static final String READY_FOR_CLUSTERING_FILE_TAGENAME = "readyForClusteringFile";
    private static final String QUERY_SIMILARITIES_FILE_TAGENAME = "querySimilaritesFile";
    private static final String CLUSTERED_QUERIES_OUTPUT_FILE_TAGENAME = "clusteredQueriesOutputFile";
    private static final String CLUSTERED_QUERIES_INPUT_FILE_TAGENAME = "clusteredQueriesInputFile";
    private static final String QUERY_IDS_INPUT_FILE_TAGENAME = "queryIDsFile";
    private static final String QUERY_EXECUTION_TIME_FILE_TAGENAME = "queryExecutionTimeFile";
    private static final String SESAME_INPUT_TRIPLES_FILE_TAGENAME = "sesameInputTriplesFile";
    private static final String SESAME_SERVER_ADDRESS_TAGENAME = "sesameServerAddress";
    private static final String VIRTUOSO_INPUT_TRIPLES_FILE_TAGENAME = "virtuosoInputTriplesFile";
    private static final String EXTENDED_DATASET_INPUT_TRIPLES_FILE_TAGENAME = "extendedDatasetInputTriplesFile";
    private static final String EXTENDED_DATASET_OUTPUT_TRIPLES_FILE_TAGENAME = "extendedDatasetOutputTriplesFile";
    private static final String QUERIES_INPUT_FILE_TAGENAME = "queriesInputFile";
    private static final String JENATDB_INPUT_TRIPLES_FILE_TAGENAME = "jenaTDBInputTriplesFile";
    private static final String LEAST_FREQUENCY_FOR_QUERY_TAGNAME = "leastFrequencyForQuery";
    private static final String SESAME_REPOSITORY_ID_TAGNAME = "sesameRepositoryID";
    private static final String JENATDB_DATASET_GRAPH_TAGNAME = "jenaTDBDatasetGraph";
    private static final String WARMUP_TIME_PERIOD_TAGNAME = "warmupTimePeriod";
    private static final String ACTUAL_RUNNING_TIME_PERIOD_TAGNAME = "actualRunningTimePeriod";
    private static final String QUERY_EXECUTION_TIME_LIMIT_TAGNAME = "queryExecutionTimeLimit";
    private static final String ERROR_FILE_TAGNAME = "errorFile";
    private static final String JENATTDB_SERVER_ADDRESS_TAGNAME = "jenaTDBServerAddress";
    private static final String VIRTUOSO_SERVER_ADDRESS_TAGENAME = "virtuosoServerAddress";

    private static final String BIGOWLIM_SERVER_ADDRESS_TAGNAME = "bigOWLIMServerAddress";
    private static final String BIGOWLIM_REPOSITORY_ID_TAGNAME = "bigOWLIMRepositoryID";
    private static final String RUNNING_TIMES_FILE_TAGNAME = "runningTimesFile";
    private static final String TRIPLESTORE_TO_TEST_TAGNAME = "tripleStoreToTest";

    public static float percentageOfClassesConsidered = 0;
    public static float percentageOfDataRequired = 0;
    public static int numberOfTriples = 0;


    public static boolean includeSubresources = false;
    public static String dsn = "";
    public static String username = "";
    public static String password = "";
    public static String queryLogFolder = "";
    public static String storeType = "";
    public static String outputFileName = "";
    public static String outputFileFormat = "";
    public static String sparqlEndpoint = "";
    public static String extractionMethod = "";
    public static String classesInputFilename = "";
    public static String triplesInputFoldername = "";
    public static boolean decompressFiles = false;
    public static String sortedQueriesOutputFile = "";
    public static String readyForClusteringFile = "";
    public static String querySimilaritesFile = "";
    public static String clusteredQueriesInputFile = "";
    public static String clusteredQueriesOutputFile = "";
    public static String queryIDsFile = "";
    public static String queryExecutionTimeFile = "";
    public static String sesameInputTriplesFile = "";
    public static String sesameServerAddress = "";
    public static String virtuosoServerAddress = "";
    public static String virtuosoInputTriplesFile = "";
    public static String extendedDatasetInputTriplesFile = "";
    public static String extendedDatasetOutputTriplesFile = "";
    public static String queriesInputFile = "";
    public static String jenaTDBInputTriplesFile = "";
    public static int leastFrequencyForQuery = 10;
    public static String sesameRepositoryID = "";
    public static String jenaTDBDatasetGraph = "";
    public static int warmupTimePeriod = 1; //default value
    public static int actualRunningTimePeriod = 5; //default value
    public static int queryExecutionTimeLimit = 3; //default value
    public static String errorFile = "";
    public static String jenaTDBServerAddress = "";
    public static String bigOWLIMServerAddress = "";
    public static String bigOWLIMRepositoryID = "";
    public static String runningTimesFile = "";
    public static String tripleStoreToTest = "virtuoso"; //Default value

    static{
        try{
            dbFactory = DocumentBuilderFactory.newInstance();
            dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(new File(benchmarkConfigFile));
        }
        catch(Exception exp){
            logger.error(exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the percentage of all Ontology classes that should be processed 
     */
    private static void _readPercenatgeOfClassesConsidered() {
        percentageOfClassesConsidered = Float.parseFloat(
                doc.getElementsByTagName(PERCENTAGE_OF_CLASSES_CONSIDERED_TAGNAME).item(0).getTextContent().trim());
    }

    /**
     * Reads the value of the percentage of all instances of a specific Ontology class that should be processed
     */
    private static void _readPercentageOfDataRequired() {
        percentageOfDataRequired = Float.parseFloat(
                doc.getElementsByTagName(PERCENTAGE_OF_DATA_REQUIRED_TAGNAME).item(0).getTextContent().trim());
    }

    private static void _readStoreInfo() {
        NodeList storeNodes = doc.getElementsByTagName(STORES_TAGNAME);
        for(int i=0; i<storeNodes.getLength(); i++){

            Element elemStore = (Element)storeNodes.item(i);
            storeType = elemStore.getAttribute(TYPE_ATTRIBUTENAME);
            dsn = elemStore.getElementsByTagName(DSN_TAGNAME).item(0).getTextContent().trim();
            username = elemStore.getElementsByTagName(USERNAME_TAGNAME).item(0).getTextContent().trim();
            password = elemStore.getElementsByTagName(PASSWORD_TAGNAME).item(0).getTextContent().trim();
        }
    }

    /**
     * Reads the value of the filename along with the required file format
     */
    private static void _readOutputFileInfo() {
        try{
            outputFileFormat = ((Element)doc.getElementsByTagName(OUTPUT_FILE_TAGNAME).item(0)).getAttribute(TYPE_ATTRIBUTENAME);
            outputFileName = doc.getElementsByTagName(OUTPUT_FILE_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Output filename cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads all configurations from the configuration file  
     */
    public static void readBenchmarkConfiguration(){
        try{
            _readPercenatgeOfClassesConsidered();
            _readPercentageOfDataRequired();
            _readOutputFileInfo();
            _readIncludeSubresources();
            _readSparqlEndpoint();
            _readStoreInfo();
            _readNumberOfTriples();
            _readExtractionMethod();
            _readClassesInputFileName();
            _readTriplesInputFolderName();
            _readDecompressFiles();
            _readQueryLogFolder();
            _readSortedQueriesOutputFile();
            _readReadyForClusteringFile();
            _readQuerySimilaritiesFile();
            _readClusteredQueriesInputFile();
            _readClusteredQueriesOutputFile();
            _readQueryIDsFile();
            _readQueryExecutionTimeFile();
            _readSesameInputTriplesFile();
            _readSesameServerAddress();
            _readVirtuosoInputTriplesFile();
            _readExtendedDatasetInputTriplesFile();
            _readExtendedDatasetOutputTriplesFile();
            _readQueriesInputFile();
            _readJenaTDBInputTriplesFile();
            _readLeastFrequencyForQuery();
            _readSesameRepositoryID();
            _readJenaTDBDatasetGraph();
            _readActualRunningTimePeriod();
            _readWarmupTimePeriod();
            _readQueryExecutionTimeLimit();
            _readRunningTimesFile();
            _readErrorFile();
            _readJenaTDBServerAddress();
            _readBigOWLIMServerAddress();
            _readBigOWLIMRepositoryID();
            _readTriplestoreToTest();
            _readVirtuosoServerAddress();

            
            logger.info("Configuration file successfully read");
        }
        catch(Exception exp){
            logger.error("Configuration file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value indicating whether to include subresources in the selected data or not
     */
    private static void _readIncludeSubresources(){
        try{
            includeSubresources = Boolean.parseBoolean(doc.getElementsByTagName(INCLUDE_SUBRESOURCES_TAGNAME).item(0).getTextContent().trim());
        }
        catch(Exception exp){
            logger.error("Value of include subresources cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the SPARQL endpoint from which we should extract our data 
     */
    private static void _readSparqlEndpoint() {
        try{
            sparqlEndpoint = doc.getElementsByTagName(SPARQL_ENDPOINT_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("SPARQL endpoint cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the number of triples that should be generated
     */
    private static void _readNumberOfTriples() {
        try{
            numberOfTriples = Integer.parseInt(doc.getElementsByTagName(NUMBER_OF_TRIPLES_TAGNAME).item(0).getTextContent().trim());
        }
        catch(Exception exp){
            logger.error("Number of triples cannot be read due to " + exp.getMessage(), exp);
        }
    }

     /**
     * Reads the value of the extraction method to be used
     */
    private static void _readExtractionMethod() {
        try{
            extractionMethod = doc.getElementsByTagName(EXTRACTION_METHOD_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Extraction method cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file in which the ontology classes are stored
     */
    private static void _readClassesInputFileName() {
        try{
            classesInputFilename = doc.getElementsByTagName(CLASSES_INPUT_FILENAME_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Classes input filename cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the FolderName in which triples are stored
     */
    private static void _readTriplesInputFolderName() {
        try{
            triplesInputFoldername = doc.getElementsByTagName(TRIPLES_INPUT_FOLDERNAME_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Triples input filename cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of indicating whether the application should decompress the input files or not
     */
    private static void _readDecompressFiles() {
        try{
            decompressFiles = Boolean.parseBoolean(doc.getElementsByTagName(DECOMPRESS_FILES_TAGENAME).item(0).getTextContent().trim());
        }
        catch(Exception exp){
            logger.error("Decompress files cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the folder in which the _query log is stored
     */
    private static void _readQueryLogFolder() {
        try{
            queryLogFolder = doc.getElementsByTagName(QUERY_LOG_FOLDER_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Query log folder cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file in which the sorted queries will be stored
     */
    private static void _readSortedQueriesOutputFile() {
        try{
            sortedQueriesOutputFile = doc.getElementsByTagName(SORTED_QUERIES_OUTPUT_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Sorted output file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file in which the remaining queries after discarding the least frequent queries are discarded 
     */
    private static void _readReadyForClusteringFile() {
        try{
            readyForClusteringFile = doc.getElementsByTagName(READY_FOR_CLUSTERING_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Ready for clustering file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file in which the remaining queries after discarding the least frequent queries are discarded
     */
    private static void _readQuerySimilaritiesFile() {
        try{
            querySimilaritesFile = doc.getElementsByTagName(QUERY_SIMILARITIES_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Query similarities file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that contains the output of the clustering tool i.e. BorderFlow
     */
    private static void _readClusteredQueriesInputFile() {
        try{
            clusteredQueriesInputFile = doc.getElementsByTagName(CLUSTERED_QUERIES_INPUT_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Clustered-Queries-Input file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that will contain the output of the clustering tool after removing the small clusters
     */
    private static void _readClusteredQueriesOutputFile() {
        try{
            clusteredQueriesOutputFile = doc.getElementsByTagName(CLUSTERED_QUERIES_OUTPUT_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Clustered-Queries-Output file cannot be read due to " + exp.getMessage(), exp);
        }
    }

     /**
     * Reads the value of the file that will contain each _query along its ID
     */
    private static void _readQueryIDsFile() {
        try{
            queryIDsFile = doc.getElementsByTagName(QUERY_IDS_INPUT_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Queries IDs file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that will contain the execution time of each _query against the SPARQL end-point
     */
    private static void _readQueryExecutionTimeFile() {
        try{
            queryExecutionTimeFile = doc.getElementsByTagName(QUERY_EXECUTION_TIME_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Query execution time file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that is used as input file for loading a Sesame triplestore with data, and normally
     * its format is N-TRIPLES
     */
    private static void _readSesameInputTriplesFile() {
        try{
            sesameInputTriplesFile = doc.getElementsByTagName(SESAME_INPUT_TRIPLES_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Sesame input triples file cannot be read due to " + exp.getMessage(), exp);
        }
    }

     /**
     * Reads the address of the Sesame server
     */
    private static void _readSesameServerAddress() {
        try{
            sesameServerAddress = doc.getElementsByTagName(SESAME_SERVER_ADDRESS_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Sesame server address cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that is used as input file for loading a Virtuoso triplestore with data, and normally
     * its format is N-TRIPLES
     */
    private static void _readVirtuosoInputTriplesFile() {
        try{
            virtuosoInputTriplesFile = doc.getElementsByTagName(VIRTUOSO_INPUT_TRIPLES_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Virtuoso input triples file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that is used as input file for generating another copy of DBpedia dataset by renaming
     * URIs
     */
    private static void _readExtendedDatasetInputTriplesFile() {
        try{
            extendedDatasetInputTriplesFile = doc.getElementsByTagName(EXTENDED_DATASET_INPUT_TRIPLES_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Extended dataset input triples file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that is used as output file for generating another copy of DBpedia dataset by renaming
     * URIs
     */
    private static void _readExtendedDatasetOutputTriplesFile() {
        try{
            extendedDatasetOutputTriplesFile = doc.getElementsByTagName(EXTENDED_DATASET_OUTPUT_TRIPLES_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Extended dataset output triples file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that contains each SPARQL _query along with the _query that causes some variability in it
     */
    private static void _readQueriesInputFile() {
        try{
            queriesInputFile = doc.getElementsByTagName(QUERIES_INPUT_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Queries input file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the file that is used as input file for loading a Sesame triplestore with data, and normally
     * its format is N-TRIPLES
     */
    private static void _readJenaTDBInputTriplesFile() {
        try{
            jenaTDBInputTriplesFile = doc.getElementsByTagName(JENATDB_INPUT_TRIPLES_FILE_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Jena TDB input triples file cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the frequency below which the query is discarded.
     */
    private static void _readLeastFrequencyForQuery() {
        try{
            leastFrequencyForQuery = Integer.parseInt(doc.getElementsByTagName(LEAST_FREQUENCY_FOR_QUERY_TAGNAME).item(0).
                    getTextContent().trim());
        }
        catch(Exception exp){
            logger.error("The value of least frequency for query cannot be read, due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value the sesame repository to which data will be loaded.
     */
    private static void _readSesameRepositoryID() {
        try{
            sesameRepositoryID = doc.getElementsByTagName(SESAME_REPOSITORY_ID_TAGNAME).item(0).
                    getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("The ID of sesame repository cannot be read, due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value the dataset of Jena TDB to which data will be loaded.
     */
    private static void _readJenaTDBDatasetGraph() {
        try{
            jenaTDBDatasetGraph = doc.getElementsByTagName(JENATDB_DATASET_GRAPH_TAGNAME).item(0).
                    getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("The dataset graph of Jena TDB cannot be read, due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the time period needed for warming a triplestore up.
     */
    private static void _readWarmupTimePeriod() {
        try{
            warmupTimePeriod = Integer.parseInt(doc.getElementsByTagName(WARMUP_TIME_PERIOD_TAGNAME).item(0).
                    getTextContent().trim());
        }
        catch(Exception exp){
            logger.error("The value of warmup period cannot be read, due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the time period needed for running the queries against a triplestore.
     */
    private static void _readActualRunningTimePeriod() {
        try{
            actualRunningTimePeriod = Integer.parseInt(doc.getElementsByTagName(ACTUAL_RUNNING_TIME_PERIOD_TAGNAME).item(0).
                    getTextContent().trim());
        }
        catch(Exception exp){
            logger.error("The value of actual running time period cannot be read, due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value of the time limit for running a query.
     */
    private static void _readQueryExecutionTimeLimit() {
        try{
            queryExecutionTimeLimit = Integer.parseInt(doc.getElementsByTagName(QUERY_EXECUTION_TIME_LIMIT_TAGNAME).item(0).
                    getTextContent().trim());
        }
        catch(Exception exp){
            logger.error("The value of query execution time limit cannot be read, due to " + exp.getMessage(), exp);
        }
    }

     /**
     * Reads the filename of the file to which errors will e written
     */
    private static void _readErrorFile() {
        try{
            errorFile = doc.getElementsByTagName(ERROR_FILE_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("The error filename cannot be read, due to " + exp.getMessage(), exp);
        }
    }

     /**
     * Reads the value the address of Jena TDB server to which data will be loaded.
     */
    private static void _readJenaTDBServerAddress() {
        try{
            jenaTDBServerAddress = doc.getElementsByTagName(JENATTDB_SERVER_ADDRESS_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Jena-TDB server address cannot be read, due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value the address of Virtuoso server to which data will be loaded.
     */
    private static void _readVirtuosoServerAddress() {
        try{
            virtuosoServerAddress = doc.getElementsByTagName(VIRTUOSO_SERVER_ADDRESS_TAGENAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("Virtuoso server address cannot be read, due to " + exp.getMessage(), exp);
        }
    }


    /**
     * Reads the address of the BigOWLIM server
     */
    private static void _readBigOWLIMServerAddress() {
        try{
            bigOWLIMServerAddress = doc.getElementsByTagName(BIGOWLIM_SERVER_ADDRESS_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("BigOWLIM server address cannot be read due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value the BigOWLIM repository that will be queried
     */
    private static void _readBigOWLIMRepositoryID() {
        try{
            bigOWLIMRepositoryID = doc.getElementsByTagName(BIGOWLIM_REPOSITORY_ID_TAGNAME).item(0).
                    getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("The ID of BigOWLIM repository cannot be read, due to " + exp.getMessage(), exp);
        }
    }

    /**
     * Reads the value the dataset of Jena TDB to which data will be loaded.
     */
    private static void _readRunningTimesFile() {
        try{
            runningTimesFile = doc.getElementsByTagName(RUNNING_TIMES_FILE_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("The running times filename cannot be read, due to " + exp.getMessage(), exp);
        }
    }

     /**
     * Reads the name of the triplestore to be tested
     */
    private static void _readTriplestoreToTest() {
        try{
            tripleStoreToTest = doc.getElementsByTagName(TRIPLESTORE_TO_TEST_TAGNAME).item(0).getTextContent().trim();
        }
        catch(Exception exp){
            logger.error("The name of the triplestore cannot be read, due to " + exp.getMessage(), exp);
        }
    }

}
