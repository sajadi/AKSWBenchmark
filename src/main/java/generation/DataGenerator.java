package generation;

import com.hp.hpl.jena.rdf.model.RDFNode;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.helper.FileUtilities;
import org.benchmark.helper.ModelUtilities;
import org.benchmark.ontology.InstanceSelector;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Dec 19, 2010
 * Time: 5:52:25 PM
 * Data generator for DBpedia datasets of different sizes
 */
public class DataGenerator {

    private static Logger logger = Logger.getLogger(DataGenerator.class);

    /**
     * Generates the data using the generation method set in the configuration file
     */
    public static void generateData() {
        int numberOfTriplesToBeGenerated = (int)(ModelUtilities.getTotalDBpediaTriples() *
                (BenchmarkConfigReader.percentageOfDataRequired /100));


        if(BenchmarkConfigReader.extractionMethod.compareTo("RandomInstance") == 0){
            //List of visited nodes, in order not process the same nodes more than once 
            ArrayList<String> visitedNodesList = ModelUtilities.getOriginalVisitedNodesInFile();
//            ArrayList<RDFNode> visitedNodesList = new ArrayList<RDFNode> ();
            logger.info("NUMBER OF VISITED NODES = " + visitedNodesList.size());

            InstanceSelector selector = new InstanceSelector();
            selector.loadClassInstancesFromFile(BenchmarkConfigReader.classesInputFilename);

            //This variable is used as an indicator to indicate whether the last node was processed successfully
            //so we can go to another one or we should try with same node again
            boolean lastNodeProcessedSuccessfully = true;

            RDFNode node = selector.getRandomInstanceFromFile();
            
            while((node != null) && (ModelUtilities.getNumberOfTriples() < numberOfTriplesToBeGenerated)){
                try{
                    if(lastNodeProcessedSuccessfully){
                        logger.info("# of Triples written = " + ModelUtilities.getNumberOfTriples());

                        //Keep selecting a random node until we encounter an unvisited node
                        boolean firstTime = true;
                        do{
                            if(firstTime){
                                firstTime = false;
                            }
                            else{
                                logger.info("INSTANCE IS ALREADY VISITED BEFORE, SO WE SHOULD SELECT ANOTHER ONE");
                            }

                            node = selector.getRandomInstanceFromFile();
                            //logger.info("NODE = " + node.toString());
                        }while(visitedNodesList.contains(node.toString()));

                    }
//                    ModelUtilities.generateTripleForInstance(node);
                    RandomInstance.generateTripleForInstance(node);
                    lastNodeProcessedSuccessfully = true;
                }
                catch(Exception exp){
                    logger.error("Error processing node titled = " + node, exp);
                    lastNodeProcessedSuccessfully = false;
                }
            }
        }
        else if(BenchmarkConfigReader.extractionMethod.compareTo("RandomTriple") == 0){
            /*while(RandomTriple.getNumberOfTriples() < numberOfTriplesToBeGenerated){
                RandomTriple.generateTriple();
            } */
            if(BenchmarkConfigReader.decompressFiles)
                FileUtilities.decompressAllFiles();
//            FileUtilities.iterateThroughFiles(new String[]{"nt"});

            Collection files = FileUtilities.iterateThroughFiles(new String[]{"nt"});
            for (Iterator iterator = files.iterator(); iterator.hasNext();) {
                File file = (File) iterator.next();
                RandomTriple.readTriplesFromFile(file.getAbsolutePath());
            }
//            RandomTriple.readTriplesFromFile();
        }
        else{
            logger.error("Unknown extraction method, program should terminate");
            System.exit(1);
        }

        /*ModelUtilities.readTotalDBpediaTriples();*/
    }

}
