package org.benchmark.helper;

import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Sep 16, 2010
 * Time: 5:29:58 PM
 * This class generates the MD5 hash code for a triple or a node
 */
public class MD5HashGenerator {

    //Initializing the Logger
    private static Logger logger = null;

    static
    {
        try
        {
            logger = Logger.getLogger(MD5HashGenerator.class);
        }
        catch (Exception exp){

        }
    }

    public static String getMD5HashCode(Statement requiredStatement)
    {
        String hashCode = null;
        try
        {
            MessageDigest algorithm = MessageDigest.getInstance("MD5");

            algorithm.reset();
            algorithm.update(requiredStatement.toString().getBytes());

            byte messageDigest[] = algorithm.digest();

            hashCode = getHexString(messageDigest);

        }
        catch(NoSuchAlgorithmException exp){
            logger.error("FAILED to create hash code for " + requiredStatement.toString());
        }
        catch(Exception exp){
            logger.error(exp.getMessage());
        }
        return hashCode;
    }

    public static String getMD5HashCode(String requiredString)
    {
        String hashCode = null;
        try
        {
            MessageDigest algorithm = MessageDigest.getInstance("MD5");

            algorithm.reset();
            algorithm.update(requiredString.getBytes());

            byte messageDigest[] = algorithm.digest();

            hashCode = getHexString(messageDigest);

        }
        catch(NoSuchAlgorithmException exp){
            logger.error("FAILED to create hash code for " + requiredString);
        }
        catch(Exception exp){
            logger.error(exp.getMessage());
        }
        return hashCode;
    }

    public static String getHexString(byte[] b) throws Exception {
      String result = "";
      for (int i=0; i < b.length; i++) {
        result +=
              Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
      }
      return result;
    }

}
