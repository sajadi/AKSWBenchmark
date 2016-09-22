package org.benchmark.querying;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Nov 12, 2010
 * Time: 4:53:03 PM
 * Interface for QueryExecutor that will be implemented for different triplestores
 */
public interface QueryExecutor {
    /**
     * Executes the passed _query and returns its execution time
     * @param sparqlQuery The _query that should be executed, along with its variation
     * @return  The execution time of the _query
     */
    double executeQuery(SPARQLQuery sparqlQuery);

    /**
     * Executes the passed _query only for warming the system up
     * @param sparqlQuery The _query that should be executed
     */
    void executeWarmUpQuery(SPARQLQuery sparqlQuery);
}
