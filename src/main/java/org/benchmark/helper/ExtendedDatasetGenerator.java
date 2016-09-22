package org.benchmark.helper;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 25, 2010
 * Time: 11:30:00 AM
 * Generates another 100% dataset from DBpedia dataset, by renaming the URIs, e.g. from <http://dbpedia.org/resource/Germany>
 * to <http://dbpedia2.org/resource/Germany>.
 */
public class ExtendedDatasetGenerator {
    private static Logger logger = Logger.getLogger(ExtendedDatasetGenerator.class);
    public static void generateExtendedDataset(){
        File file = null;
        FileReader fReader = null;
        Random generator = new Random();
        FileOutputStream fWriter = null;

        int lineIndex = 0;
        ArrayList<String> lines = new ArrayList<String>(10);
        String line = "";

        try{
            //Input file
            file = new File(BenchmarkConfigReader.extendedDatasetInputTriplesFile);
            fReader = new FileReader(file);

            LineNumberReader lnReader = new LineNumberReader(fReader);
            fWriter = new FileOutputStream(BenchmarkConfigReader.extendedDatasetOutputTriplesFile, true);
            int numberOfTriplesExtracted = 0;

            logger.info("Extraction of triples from file has started");
            while ((line = lnReader.readLine()) != null){
                try{
//                    if(line.charAt(line.length()-1) != '.')
//                    logger.info("not ending with DOT");
                    line += "\n";

                    numberOfTriplesExtracted ++;
                    line = line.replace("http://dbpedia.org/", "http://dbpedia2.org/");
//                    line = StringUtils.replace(line, "http://dbpedia.org/resource", "http://dbpedia2.org/resource");
                    fWriter.write(line.getBytes());
//                    logger.info("One triple has been processed");
                    if(numberOfTriplesExtracted % 50000 == 0)
                        logger.info("50K triples are successfully processed....");
                }
                catch(Exception exp){
                    logger.error("Triple " + line + " cannot be processed, due to " + exp.getMessage(), exp);
                }
            }
            if(fReader != null)
                fReader.close();

            if(fWriter != null)
                fReader.close();
            
            logger.info(numberOfTriplesExtracted + " triples are successfully extracted at random from file ");
        }
        catch(Exception exp){
            logger.error("File " + BenchmarkConfigReader.extendedDatasetInputTriplesFile + " cannot be processed, due" +
                    " to " + exp.getMessage(), exp);

        }
    }

    public static int countTriplesinExtendedDataset(){
        File file = null;
        FileReader fReader = null;
        int numberOfTriplesExtracted = 0;


        int lineIndex = 0;
        ArrayList<String> lines = new ArrayList<String>(10);
        String line = "";

        try{
            //Input file
            file = new File(BenchmarkConfigReader.extendedDatasetOutputTriplesFile);
            fReader = new FileReader(file);

            LineNumberReader lnReader = new LineNumberReader(fReader);
            numberOfTriplesExtracted = 0;

            logger.info("Counting of triples from file has started");
            boolean first = true;
            String oldLine = "";
            while ((line = lnReader.readLine()) != null){
                if(first){
                    oldLine = line;
                    first = false;
                }
                try{

                    numberOfTriplesExtracted ++;
                    if(oldLine.compareTo(line) == 0)
                        logger.info(numberOfTriplesExtracted +"  " + line);
//                    line = StringUtils.replace(line, "http://dbpedia.org/resource", "http://dbpedia2.org/resource");
//                    logger.info(numberOfTriplesExtracted + "\r\n");
                }
                catch(Exception exp){
                    logger.error("Triple " + line + " cannot be processed, due to " + exp.getMessage(), exp);
                }
            }
            if(fReader != null)
                fReader.close();


            logger.info(numberOfTriplesExtracted + " triples are in the file ");
        }
        catch(Exception exp){
            logger.error("File " + BenchmarkConfigReader.extendedDatasetInputTriplesFile + " cannot be processed, due" +
                    " to " + exp.getMessage(), exp);

        }
        return numberOfTriplesExtracted;
    }

    public static int countResourceTriplesInDataset(){
        File file = null;
        FileReader fReader = null;
        int numberOfTriplesExtracted = 0;


        int lineIndex = 0;
        String line = "";

        try{
            //Input file
            file = new File(BenchmarkConfigReader.extendedDatasetInputTriplesFile);
            fReader = new FileReader(file);

            LineNumberReader lnReader = new LineNumberReader(fReader);
            numberOfTriplesExtracted = 0;

            logger.info("Counting of triples from file has started");
            boolean first = true;
            String oldLine = "";
            while ((line = lnReader.readLine()) != null){

                try{

                    numberOfTriplesExtracted ++;
                    if(!line.contains("http://dbpedia.org/resource")){

                        logger.info("NOT RESOURCE.............");
                        logger.info(line);
                    }
                }
                catch(Exception exp){
                    logger.error("Triple " + line + " cannot be processed, due to " + exp.getMessage(), exp);
                }
            }
            if(fReader != null)
                fReader.close();


            logger.info(numberOfTriplesExtracted + " triples are in the file ");
        }
        catch(Exception exp){
            logger.error("File " + BenchmarkConfigReader.extendedDatasetInputTriplesFile + " cannot be processed, due" +
                    " to " + exp.getMessage(), exp);

        }
        return numberOfTriplesExtracted;
    }
}
