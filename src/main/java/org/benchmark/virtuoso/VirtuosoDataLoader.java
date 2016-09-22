package org.benchmark.virtuoso;

import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.helper.JDBC;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 18, 2010
 * Time: 11:06:34 AM
 * Loads RDF data to a Virtuoso triplestore.
 */
public class VirtuosoDataLoader {

    private static Logger logger = Logger.getLogger(VirtuosoDataLoader.class);


    public static void loadData(){

        try {
            JDBC jdbc = JDBC.getDefaultConnection();

//            File inputFile = new File(BenchmarkConfigReader.virtuosoInputTriplesFile);
            File inputFile = new File(BenchmarkConfigReader.virtuosoInputTriplesFile);
            FileReader fReader = new FileReader(inputFile);
            LineNumberReader lnReader = new LineNumberReader(fReader);

            File tempFile = new File(inputFile.getParent() + "/temp.nt");
            
            String strWritingDataStatement;
            
            try {
                while(readPartOfInputFile(lnReader, tempFile)){
                    long startTime = System.nanoTime();
                    logger.info("******************VIRTUOSO LOADER***********************");
                    String tempFileName = tempFile.getAbsolutePath().replace("\\","/");
                    strWritingDataStatement = "ttlp (file_to_string_output('" + tempFileName +"')" +
                            ", '', 'http://dbpedia.org', 0)";

                    jdbc.exec(strWritingDataStatement);

                    double endTime = (System.nanoTime() - startTime)/1000000.0;

                    logger.info("1000 triples are written successfully,  " //+ BenchmarkConfigReader.virtuosoInputTriplesFile
                          +" and it took " + endTime + " Micro-Seconds");
                    tempFile = new File(inputFile.getParent() + "/temp.nt");
                }

                logger.info("Data loaded successfully");

           }
           finally {
                lnReader.close();
                tempFile.delete();
           }

           logger.info("Data loaded into virtuoso successfully");
        }
        catch (Exception exp) {
           logger.error("Failed to load data to virtuoso, due to " + exp.getMessage(), exp);

        }
    }

    /**
     * Reads 1000 Triples from the input file and places them into the output file, in order to facilitate inserting
     * the contents of large input N-TRIPLES file into Virtuoso, as very large files block the insertion process
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
                if(numberOfLines > 1000)
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
            return false;
        }

    }

}
