package org.benchmark.virtuoso;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Dec 11, 2010
 * Time: 8:49:11 PM
 * Calculates the statistics of a given dataset, i.e. indgegree and outdegree
 */
public class Statistics {

    private static Logger logger = Logger.getLogger(Statistics.class);
    public static final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";

    public static void calculateOutDegree(){
        try{
            QueryEngineHTTP queryEngine = null;
            int numberOfResults = 0;

            long lStartTime = new Date().getTime();
            File outdegreeFile = new File("/opt/akswbenchmark/queries/outdegree.txt");
            FileWriter outWriter = new FileWriter(outdegreeFile);

            while(true){
                String query = "SELECT ?s, COUNT(?o) { ?s ?p ?o. } GROUP BY ?s OFFSET " + numberOfResults +" LIMIT 1000000";
    //            String query = "SELECT ?o, count(?s) { ?s ?p ?o. Filter(isiri(?o) && ?p != <http://dbpedia.org/property/wikilink>) } group by ?o";
    //            "\n" +
    //                    "SELECT ?x, COUNT(?z) { ?x ?y ?z. } group by ?x limit 10"
    //            queryEngine = new QueryEngineHTTP("http://139.18.2.96:8898/sparql", query);
                  queryEngine = new QueryEngineHTTP("http://dbpedia.aksw.org:8999/sparql", query);


                logger.info("Calculation of OUTDEGREE started at " + now());
    

                ResultSet rs = queryEngine.execSelect();
                boolean isEndOfResults = true; //This is an indicator, that indicates whether we reached the last
                        //statement or not, as if it goes into loop this means that the end is not yet reached

                while(rs.hasNext()){
                    isEndOfResults = false;
                    QuerySolution qs = rs.nextSolution();
                    Iterator<String> variables = qs.varNames();
                    while(variables.hasNext()){
                        outWriter.write(qs.get(variables.next()) + "\t");
                    }
                    outWriter.write("\n");
                    numberOfResults++;
                }
                logger.info(numberOfResults + " statistics are successfully written into file");

                if(isEndOfResults)
                    break;
                
            }


            long lEndTime = new Date().getTime();

            logger.info("Calculation of OUTDEGREE ended at " + now() + 
                    ",and it took " + (lEndTime-lStartTime)/1000 + " sec");
            outWriter.flush();
            outWriter.close();
                    
        }
        catch(Exception exp){
            logger.error("Failed to compute outdgree, due to " + exp.getMessage(), exp);
        }

    }


    public static void calculateInDegree(){
        try{
            QueryEngineHTTP queryEngine = null;
            int numberOfResults = 0;
            File indegreeFile = new File("/opt/akswbenchmark/queries/indegree.txt");
            FileWriter outWriter = new FileWriter(indegreeFile);

            long lStartTime = new Date().getTime();

//            String query = "SELECT ?s, COUNT(?o) { ?s ?p ?o. } GROUP BY ?s";
            while(true){

                String query = "SELECT ?o, count(?s) { ?s ?p ?o. Filter(isiri(?o)) } group by ?o OFFSET " + numberOfResults + " LIMIT 1000000";
    //            "\n" +
    //                    "SELECT ?x, COUNT(?z) { ?x ?y ?z. } group by ?x limit 10"
                //queryEngine = new QueryEngineHTTP("http://139.18.2.96:8898/sparql", query);
                  queryEngine = new QueryEngineHTTP("http://dbpedia.aksw.org:8999/sparql", query);


                logger.info("Calculation of INTDEGREE started at " + now());


                boolean isEndOfResults = true; //This is an indicator, that indicates whether we reached the last
                        //statement or not, as if it goes into loop this means that the end is not yet reached

                ResultSet rs = queryEngine.execSelect();
                while(rs.hasNext()){

                     isEndOfResults = false;

                    QuerySolution qs = rs.nextSolution();
                    Iterator<String> variables = qs.varNames();
                    while(variables.hasNext()){
                        outWriter.write(qs.get(variables.next()) + "\t");
                    }
                    outWriter.write("\n");
                    numberOfResults++;
                }

                logger.info(numberOfResults + " statistics are successfully written into file");

                if(isEndOfResults)
                    break;

            }


            long lEndTime = new Date().getTime();

            logger.info("Calculation of INDEGREE ended at " + now() +
                    ", and it took "  + (lEndTime-lStartTime)/1000 + " sec");
            outWriter.flush();
            outWriter.close();

        }
        catch(Exception exp){
            logger.error("Failed to compute indgree, due to " + exp.getMessage(), exp);
        }
    }



  public static String now() {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
    return sdf.format(cal.getTime());

  }

}
