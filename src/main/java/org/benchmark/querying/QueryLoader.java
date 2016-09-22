package org.benchmark.querying;

import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 30, 2010
 * Time: 8:17:01 PM
 * Loads the queries that will be executed against the triplestores, along with the _query that makes some variability
 * in the original _query
 */
public class QueryLoader {
    private static Logger logger = Logger.getLogger(QueryLoader.class);

    public static ArrayList<SPARQLQuery> loadQueries(){
        ArrayList<SPARQLQuery> arrQueries = new ArrayList<SPARQLQuery>();
        try{
            FileReader inReader = new FileReader(BenchmarkConfigReader.queriesInputFile);
            LineNumberReader lnReader = new LineNumberReader(inReader);
            String line = "";
                                                        
            while ((line = lnReader.readLine()) != null){
                try{
                    if(line.compareTo("")!=0){
                        String []parts = line.split("\t");
                        String features = parts[0];
                        String query = parts[1];
                        String auxQuery = parts[2];
                        arrQueries.add(new SPARQLQuery(features, query, auxQuery));
                    }
                    else//line is empty so it's considered an ignored query
                        arrQueries.add(new SPARQLQuery("", "", ""));
                }
                catch (Exception exp){
                    logger.error("Query " + line + " cannot be read from queries file, due to " + exp.getMessage(), exp );
                }
            }
            inReader.close();

            return arrQueries;
        }
        catch (Exception exp){
            logger.error("Queries cannot be loaded from file");
            return null;
        }
    }
}
