package org.benchmark.querying;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 30, 2010
 * Time: 8:21:07 PM
 * This class contains a SPARQL _query that will be executed against all triple stores, along with another simple
 * SPARQL _query that will cause some change to the original _query, in order to avoid caching capability of the
 * triplestore. 
 */
public class SPARQLQuery {

    private static Logger logger = Logger.getLogger(SPARQLQuery.class);

    public String features;
    public String query;
    public String auxiliaryQuery;

    //We use an array of arraylists here as the auxiliary query may contain more than one variable, so we hav an array
    //with the number of variables we have, and the list of allowed values for that variable is in its corresponding arraylist 
    private ArrayList<String> []_queryVariableValues;
    private ArrayList<String> _arrVariables;

    public SPARQLQuery(String Features, String Query, String AuxiliaryQuery){
        query = Query;
        auxiliaryQuery = AuxiliaryQuery;
        features = Features;

        //If the query or auxiliary query is empty, then we should return with out further processing 
        if((Query.compareTo("") == 0) || (AuxiliaryQuery.compareTo("") == 0))
            return;

        logger.info("Auxiliary Query = " + auxiliaryQuery);

        _arrVariables = replaceVariablePlaceholders();
        _queryVariableValues =  _getResultsForAuxiliaryQuery(auxiliaryQuery,_arrVariables.size());

        prepareQueryForExecution();
    }

    private ArrayList<String> replaceVariablePlaceholders() {
        //This regular expression is used to extract all variable placeholders fom the _query
        Pattern p = Pattern.compile("\\%%.*?%%");
        Matcher matcher = p.matcher(query);

        ArrayList<String> arrVariables = new ArrayList<String>();
        while (matcher.find()) {
            System.out.println("Starting & ending index of" + matcher.group()+ ":=" +
            "start=" + matcher.start() + " end = " + matcher.end());
            if(!arrVariables.contains(matcher.group()))
                arrVariables.add(matcher.group());
        }

        Collections.sort(arrVariables);
        return arrVariables;
    }

    /*public SPARQLQuery(){
        this("", "", "");        
    }*/

    /**
     * Executes the auxiliary _query and gets a set of possible values for the variable in the original _query
     * @param auxQuery  The auxiliary _query to be executed
     * @param   numberOfRequiredVariableValues  The number of values that will replace tha variable values, as
     * some queries may contain more than one variable that should be replaced
     * @return  A random possible value for the variable
     */
    private ArrayList<String>[] _getResultsForAuxiliaryQuery(String auxQuery, int numberOfRequiredVariableValues){

        QueryEngineHTTP queryEngine = null;
        queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint, auxQuery);
//        System.out.println(auxQuery);

//        Dataset dataset = TDBFactory.createDataset(BenchmarkConfigReader.jenaTDBDatasetGraph) ;
//        QueryExecution queryEngine = QueryExecutionFactory.create(auxQuery, dataset);

        ResultSet rs = queryEngine.execSelect();


        String strValue = "";
        ArrayList<String> arrValues[] = new ArrayList[rs.getResultVars().size()];
        for(int i=0; i< arrValues.length; i++)
            arrValues[i] = new ArrayList<String>();

        //Loop over the returned results
        while(rs.hasNext()){
//            RDFNode node = rs.nextSolution().get("?var");
            QuerySolution currentSolution = rs.nextSolution();
            //Loop over the variables, as the query may contain more than one variable
            for(int i = 0; i < rs.getResultVars().size(); i++){
                RDFNode node = currentSolution.get(rs.getResultVars().get(i));

                if((node!=null) && (node.isResource()))
                    strValue = "<" + node.toString() + ">";
                else if((node!=null) && (node.isLiteral()))
                {

//                    strValue = "\"" + node.toString() + "\"";
                    String strNodeVal = node.toString();

                    if(strNodeVal.contains("^^")) //It's a integer{
                        strValue = strNodeVal.substring(0, strNodeVal.indexOf("^"));


                    /*if((node.asLiteral().getDatatypeURI() != null)&&
                            (node.asLiteral().getDatatypeURI().compareTo("http://www.w3.org/2001/XMLSchema#int") == 0)){
                        strValue = Integer.toString(node.asLiteral().getInt());
                    }*/
                    else{
                        if(node.toString().indexOf("@") != -1)
                            strValue = "\"" + node.toString().replace("@", "\"@");
                        else
                            strValue = "\"" + node.toString() + "\"";
                    }

                }

                arrValues[i].add(strValue);
            }

            
            /*for(int varIndex = 0; varIndex < arrValues.length; varIndex++){
                try{
                    org.junit.Assert.assertTrue(arrValues[varIndex].size() < 0);
                    logger.info("Executed");
                }

                catch(AssertionError assertException){
                    logger.info("Failed");
                }
            }*/

        }

//    private ArrayList<String>[] _getResultsForAuxiliaryQuery(String auxQuery, int numberOfRequiredVariableValues){
//
//        Dataset dataset = TDBFactory.createDataset(BenchmarkConfigReader.jenaTDBDatasetGraph) ;
//        QueryExecution queryExecutor = QueryExecutionFactory.create(auxQuery, dataset);
//        System.out.println(auxQuery);
//          ResultSet rs = queryExecutor.execSelect();
//        String strValue = "";
//        ArrayList<String> arrValues[] = new ArrayList[rs.getResultVars().size()];
//        for(int i=0; i< arrValues.length; i++)
//            arrValues[i] = new ArrayList<String>();
//
//        //Loop over the returned results
//        while(rs.hasNext()){
////            RDFNode node = rs.nextSolution().get("?var");
//            QuerySolution currentSolution = rs.nextSolution();
//            //Loop over the variables, as the query may contain more than one variable
//            for(int i = 0; i < rs.getResultVars().size(); i++){
//                RDFNode node = currentSolution.get(rs.getResultVars().get(i));
//
//                if((node!=null) && (node.isResource()))
//                    strValue = "<" + node.toString() + ">";
//                else if((node!=null) && (node.isLiteral()))
//                    strValue = "\"" + node.toString() + "\"";
//
//                arrValues[i].add(strValue);
//            }
//
//
//        }

        //If the auxiliary _query returns no result, we should use another _query that is guaranteed to return result
        /*if(arrValues.size()<=0){
            arrValues.clear();
            queryEngine = new QueryEngineHTTP(BenchmarkConfigReader.sparqlEndpoint,
                    "SELECT DISTINCT ?var WHERE {  ?var  rdf:type ?var1 . FILTER(?var LIKE <http://dbpedia.org/resource/%>)} limit 1000");
            rs = queryEngine.execSelect();

            while(rs.hasNext()){
                RDFNode node = rs.nextSolution().get("?var");
                if(node.isResource())
                    strValue = "<" + node.toString() + ">";
                else
                    strValue = "\"" + node.toString() + "\"";

                arrValues.add(strValue);
            }
        }*/
        /*
        ArrayList<String> outputValuesList = new ArrayList<String>();
        for(int i = 0; i < numberOfRequiredVariableValues; i++ ){
            //Generate random number to be the required position of the resource or the literal that will be selected
            Random generator = new Random();
            int randomPosition = generator.nextInt(arrValues.size());
            outputValuesList.add(arrValues.remove(randomPosition));
        }

//        return arrValues.get(randomPosition);
        return outputValuesList;
        */
        return arrValues;
    }

    /**
     * Replaces the variable placeholders in the _query with actual values from the triplestore
     */
    /*public String prepareQueryForExecution(){

        ArrayList<String> outputValuesList = new ArrayList<String>();
        ArrayList<String> queryVariableValuesBackup = new ArrayList<String>(_queryVariableValues.size());


        for(String strValue: _queryVariableValues)
            queryVariableValuesBackup.add(strValue);

        for(int i = 0; i < _arrVariables.size(); i++ ){
            //Generate random number to be the required position of the resource or the literal that will be selected
            Random generator = new Random();
            int randomPosition = generator.nextInt(queryVariableValuesBackup.size());
            outputValuesList.add(queryVariableValuesBackup.remove(randomPosition));
        }

        int varIndex = 0;
        String outputQuery = _query;
        while(varIndex < _arrVariables.size()){
            outputQuery = outputQuery.replaceAll(_arrVariables.get(varIndex), outputValuesList.get(varIndex));
            varIndex++;
        }
        return outputQuery;
    }*/


    /***
     *
     * @return  A string containing the query after replacing the placeholder(s) with values from the selected ones
     * on random basis
     */
    public String prepareQueryForExecution(){

        String outputQuery = query;

        Random generator = new Random();
        int randomPosition = generator.nextInt(_queryVariableValues[0].size());

        for(int varIndex = 0; varIndex < _queryVariableValues.length; varIndex++){
//            Random generator = new Random();
//            int randomPosition = generator.nextInt(_queryVariableValues[varIndex].size());
            outputQuery = outputQuery.replaceAll(_arrVariables.get(varIndex), _queryVariableValues[varIndex].get(randomPosition));
        }
        return outputQuery;
    }


     /***
     *
     * @return  A string containing the query after replacing the placeholder(s) with values from the selected ones
     * on sequential basis
     */
//    public String prepareQueryForExecution(){
//
//        String outputQuery = query;
//
//        for(int varIndex = 0; varIndex < _queryVariableValues.length; varIndex++){
//            outputQuery = outputQuery.replaceAll(_arrVariables.get(varIndex), _queryVariableValues[varIndex].get(0));
//            _queryVariableValues[varIndex].remove(0);
//        }
//        return outputQuery;
//    }
}
