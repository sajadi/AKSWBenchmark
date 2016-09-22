package generation;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.helper.ModelUtilities;

import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Dec 13, 2010
 * Time: 10:36:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class RandomInstance {

    private static Logger logger = Logger.getLogger(RandomInstance.class);

    private static int numberOfGeneratedTriples = ModelUtilities.getOriginalNumberOfTriplesInFile();

    public static int getNumberOfTriples(){
        return numberOfGeneratedTriples;
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
}
