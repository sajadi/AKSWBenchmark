package org.benchmark.helper;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Sep 29, 2010
 * Time: 11:35:27 AM
 * Responsible for getting all statements related to resources and writing the rdf data to files  
 */
public class ModelUtilities {
    private static Logger logger = Logger.getLogger(ModelUtilities.class);

    //This variable holds the total number of triples in the whole DBpeida
    private static int totalDBpediaTriples = 0;

    private static int numberOfGeneratedTriples = getOriginalNumberOfTriplesInFile();

    public static int getNumberOfTriples(){
        return getOriginalNumberOfTriplesInFile();
    }

    public static void generateTripleForInstance(RDFNode instance){
        Model outputModel = null;
        QueryEngineHTTP queryExecuter = null;

        String query = String.format("CONSTRUCT {?s ?p ?o} FROM <http://dbpedia.org>  WHERE { {?s ?p ?o}. " +
                    "FILTER (?s = <%s>)}", instance.toString());
        //We should place it in a loop in order to try again and again till the server responds
//        _query = " CONSTRUCT {?s ?p ?o} FROM <http://dbpedia.org>  WHERE { {?s ?p ?o}. FILTER (?s = <http://dbpedia.org/resource/Mahela_Jayawardene>)}";
        while(true){
            queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, query);
            try{
                outputModel =  queryExecuter.execConstruct();

                //Write the model to output file
                numberOfGeneratedTriples += outputModel.size();
                OutputStream stream = new FileOutputStream(BenchmarkConfigReader.outputFileName, true);
                try{
                    outputModel.write(stream , BenchmarkConfigReader.outputFileFormat);
                }
                finally {
                    stream.flush();
                    stream.close();
                }
                
                logger.info("Triples for instance " + instance +" are successfully written to file");
                break;
            }
            catch(com.hp.hpl.jena.shared.JenaException exp){
                logger.error("Triples for instance " + instance +" cannot be fetched", exp);
                break;
            }
            catch (Exception exp){
                logger.error("Query = " + query);
                logger.error("Triples for instance " + instance +" cannot be fetched", exp);
                logger.info("Trying to get it again, as the server may be down");
            }

        }
    }

    /**
     * Writes the passed model to the
     */
    public static void writeModel(Model model){
//        try{
//            model.write(new FileOutputStream(BenchmarkConfigReader.outputFileName, true), BenchmarkConfigReader.outputFileFormat);
//            logger.info("Triples for model are successfully written to file");
//        }
//        catch(Exception exp){
//            logger.error("Triples for model cannot be written", exp);
//        }

        try{
            OutputStream stream = new FileOutputStream(BenchmarkConfigReader.outputFileName, true);
            try{
                 model.write(stream, BenchmarkConfigReader.outputFileFormat);
            }
            finally {
                stream.flush();
                stream.close();
            }
            logger.info("Triples for model are successfully written to file");
        }
        catch(Exception exp){
            logger.error("Triples for model cannot be written", exp);
        }
    }

     public static int getTotalDBpediaTriples(){
        if(totalDBpediaTriples == 0)
            readTotalDBpediaTriples();
        return totalDBpediaTriples;
    }

    public static void readTotalDBpediaTriples() {
        String query = "SELECT COUNT(?s)  FROM <http://dbpedia.org>  WHERE { ?s ?p ?o}";
        QueryEngineHTTP queryExecuter = null;
        queryExecuter = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, query);
         try{
             ResultSet rs =  queryExecuter.execSelect();
             QuerySolution sol = rs.next();
             totalDBpediaTriples = sol.getLiteral(rs.getResultVars().get(0)).getInt();
             logger.info("Total DBpedia triples = " + totalDBpediaTriples);
         }
         catch (Exception exp){
             logger.error("Total number of triples in DBpedia cannot be fetched, program should terminate", exp);
             System.exit(1);
         }
    }

    /**
     * This function is important to support continuation if the program is terminated.
     * Returns the number of triples that exist originally in the file to use them as the initial number of triples
     * @return  The number of triples that exist in the file
     */
    public static int getOriginalNumberOfTriplesInFile(){
        File file = null;
        FileReader fReader = null;
        int countOfLines = 0;

        try{
            file = new File(BenchmarkConfigReader.outputFileName);
            fReader = new FileReader(file);
            LineNumberReader lnReader = new LineNumberReader(fReader);
            try{
                while (lnReader.readLine() != null)
                    countOfLines++;
            }
            finally{
                if(fReader != null)
                    fReader.close();
            }

        }
        catch(Exception exp){
            logger.warn("Original number of triples in file is not read, assuming that the file is empty");
        }
        finally {

        }

        return countOfLines;
    }

    /**
     * This function is important to support continuation if the program is terminated.
     * Returns the nodes that were already visited, so that they ar not selected again 
     * @return  A list of nodes that were visited
     */
    public static ArrayList<String> getOriginalVisitedNodesInFile(){
        File file = null;
        FileReader fReader = null;
        int countOfLines = 0;

        ArrayList<String> visitedNodes = new ArrayList<String>();
        String statement ="";

        try{
            file = new File(BenchmarkConfigReader.outputFileName);
            fReader = new FileReader(file);
            LineNumberReader lnReader = new LineNumberReader(fReader);
            try{
                while ((statement = lnReader.readLine()) != null){
                    //The subject is between <, > so we should cut it and add it to the array
                    int lessThanPos = statement.indexOf("<");
                    int greaterThanPos = statement.indexOf(">");
                    String subject = statement.substring(lessThanPos+1, greaterThanPos-1);
                    
                    if(!visitedNodes.contains(subject)) {
                        logger.info("Subject = " + subject);
                        visitedNodes.add(subject);
                    }
                }

            }
            finally{
                if(fReader != null)
                    fReader.close();
            }
            return visitedNodes;

        }
        catch(Exception exp){
            logger.warn("The visited nodes cannot be read, assuming that there is no visited nodes");
            return new ArrayList<String>();
        }
        finally {

        }


    }

}
