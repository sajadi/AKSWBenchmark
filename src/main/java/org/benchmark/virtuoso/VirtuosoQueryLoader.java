package org.benchmark.virtuoso;

import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 3, 2010
 * Time: 2:23:16 PM
 * Query loader for loading the queries that were selected as representing queries during the clustering process
 * Executes the _query and calculates its execution time
 */
public class VirtuosoQueryLoader {
    private static Logger logger = Logger.getLogger(VirtuosoQueryLoader.class);

    /**
     * Reads the actual _query code from file for the passed _query IDs
     * @param queryIDs  A list containing the IDs of the needed queries
     * @param separatorLine The separator used as a delimiter to end the _query
     * @return  A list containing the actual queries for the passed IDs 
     */
    public static HashMap<Integer, String> getQueriesForIDs(ArrayList<Integer> queryIDs, String separatorLine){

        ArrayList <String> queriesList = new ArrayList<String>();
        String currentQuery = "";
        FileReader inReader;
        LineNumberReader lnReader;
        String line;

        //Read all queries into the list called queriesList
        try{
            inReader = new FileReader(BenchmarkConfigReader.queryIDsFile);
            lnReader = new LineNumberReader(inReader);

            while ((line = lnReader.readLine()) != null){

                //We have already read a _query so we can add it to queryList
                if(line.compareTo(separatorLine) == 0){
                    queriesList.add(currentQuery);
                    currentQuery = "";
//                    System.out.println(currentQuery);
                }
                else
                    currentQuery += line +"\n";
            }

            lnReader.close();

            //Remove all unneeded queries, i.e. the queries with IDs not in queryIDs
            HashMap<Integer, String> neededQueryList = new HashMap<Integer, String>();

            for(int i = 0; i < queryIDs.size(); i++){
                String query = queriesList.get(queryIDs.get(i));
                query = getQueryPartOnly(query);
                neededQueryList.put(queryIDs.get(i), query);
            }

            queriesList.clear();

            return neededQueryList;

            //First we should split the _query using the "&" character, because this character separates the parts of the _query
            //as the other parts e.g. default-graph-uri = http%3A%2F%2Fdbpedia.org, should not be touched
//            String ampersandPattern = "&";
//            Pattern ampersandSplitter = Pattern.compile(ampersandPattern);
//            String[] queryParts = ampersandSplitter.split(_query);
//
//            boolean queryPartFound = false;
//            int queryPartIndex = 0;
//            for(String part: queryParts){
//                //The _query part, in the SPARQL _query always starts with the word _query.
//                if(part.toLowerCase().startsWith("_query")){
//                    queryPartFound = true;
//                    break;
//                }
//                queryPartIndex ++;
//            }
        }
        catch (Exception exp){
            logger.error("QueryIDs file cannot be read, due to " + exp.getMessage(), exp);
            return null;
        }
    }

//    private static String getQueryPartOnly(String _query){
//        String ampersandPattern = "&";
//        Pattern ampersandSplitter = Pattern.compile(ampersandPattern);
//        String[] queryParts = ampersandSplitter.split(_query);
//
//        try{
//            boolean queryPartFound = false;
//            int queryPartIndex = 0;
//            for(String part: queryParts){
//                //The _query part, in the SPARQL _query part always contains with the word _query.
//                if(part.toLowerCase().indexOf("_query") >= 0){
//                    queryPartFound = true;
//                    break;
//                }
//                queryPartIndex ++;
//            }
//
//
//            //the _query string may not have a _query part so we cannot continue
//            if(!queryPartFound)
//                return _query;
//
//            _query = queryParts[queryPartIndex];
//
//            int equalPosition = _query.indexOf("=");
//
//            //The _query string is in the form "_query=SLEECT .....", so we should split based on = and select the second part
//            //which is the actual _query
//            String actualQuery = _query.substring(equalPosition + 1);
//            return actualQuery;
//        }
//        catch (Exception exp){
//            logger.info("Query cannot be splited original _query is returned back");
//            return _query;
//        }
//
//    }

    private static String getQueryPartOnly(String query){
        int queryPartPosition = -1;
        try{

            String queryPart = "query=";

            //The _query part, in the SPARQL _query part always contains with the word _query.
            queryPartPosition = query.toLowerCase().indexOf(queryPart);

            //the _query string may not have a _query part so we cannot continue
            if(queryPartPosition < 0)
                throw new Exception("Query part not found");

            queryPartPosition += queryPart.length();
            //The _query must end with a "}" so we can search for the last "}" and cut that part as the end of the _query
            int lastBracePosition = query.lastIndexOf("}");

            if(lastBracePosition < 0)
                throw new Exception("Ending brace '}' not found");

            //The _query string is in the form "_query=SELECT .....", so we should split based on = and select the second part
            //which is the actual _query
            String actualQuery = query.substring(queryPartPosition, lastBracePosition+1);
            
            return actualQuery;
        }
        catch (Exception exp){
            logger.error("Query cannot be splitted original _query is returned back due to " + exp.getMessage() );
            logger.error("Query = " + query );
            return query;
        }

    }
}

