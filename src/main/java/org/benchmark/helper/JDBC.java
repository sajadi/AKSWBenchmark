package org.benchmark.helper;

import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Jun 24, 2010
 * Time: 1:01:10 PM
 * This class provides support for reading JDBC information, this information is required for live extraction
 */
public class JDBC{

    public static final int JDBC_MAX_LONGREAD_LENGTH = 8000;
    //Initializing the Logger
    private Logger logger = Logger.getLogger(this.getClass().getName());

    String dsn;
    String user;
    String pw;
    static Connection con = null;
    final int wait = 5;

    public JDBC(String DSN, String USER, String Password)
    {
        this.dsn = DSN;
        this.user = USER;
        this.pw = Password;

        if(con == null)
            this.connect();
    }

    public static JDBC getDefaultConnection()
    {
        String dataSourceName = BenchmarkConfigReader.dsn;
        String username = BenchmarkConfigReader.username;
        String password = BenchmarkConfigReader.password;

        return new JDBC(dataSourceName, username, password);
    }

    /*
    * Blocks until connection exists
    *
    * */
    public void connect(boolean debug)
    {
        try{
            //Make sure that the JDBC driver for virtuoso exists
            Class.forName("virtuoso.jdbc4.Driver");


            boolean FailedOnce = false;

            Connection Conn = DriverManager.getConnection(this.dsn, this.user, this.pw);
            //logger.log(Level.INFO, "Connection to Virtuoso has been established");
            while(Conn == null){
                logger.warn("JDBC connection to " + this.dsn + " failed, waiting for "
                                                    + wait + " and retrying");
                Thread.sleep(wait);

                Conn = DriverManager.getConnection(this.dsn, this.user, this.pw);
            }
            if(debug)
            {
                logger.info("JDBC connection re-established");
            }
            con = Conn;
        }
        catch(ClassNotFoundException exp)
        {
           logger.fatal("JDBC driver of Virtuoso cannot be loaded");
           System.exit(1);
        }
        catch(Exception exp)
        {
             logger.warn(exp.getMessage() + " Function connect ");
        }
    }

    public void connect(){
        connect(false);
    }

     /*
	 * returns the jdbc statement
	 * */
	public PreparedStatement prepare(String query)
    {
        try{
    	 	PreparedStatement result = con.prepareStatement(query);

            return result;
        }
        catch(Exception exp){

            return null;
        }
	}

    public boolean executeStatement(PreparedStatement sqlStatement, String[] parameterList)
    {
        boolean successfulExecution = false;
        try{
            if((con == null) || (con.isClosed()))
                con = DriverManager.getConnection(this.dsn, this.user, this.pw);

            for(int i=0;i<parameterList.length; i++)
            {
                sqlStatement.setString(i+1, parameterList[i]);
            }
            sqlStatement.execute();
            successfulExecution = true;
            sqlStatement.close();
            //con.close();
        }
        catch(Exception exp){
            logger.error(exp.getMessage() + " Function executeStatement ", exp);
            successfulExecution = false;
        }

        return successfulExecution;
    }

    //This function executes the passed _query
    public ResultSet exec(String query)
    {
        ResultSet result = null;
        try
        {
            if((con == null) || (con.isClosed()))
                con = DriverManager.getConnection(this.dsn, this.user, this.pw);

            Statement requiredStatement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            result = requiredStatement.executeQuery(query);
            //Timer.stop(logComponent + "::exec _query");
//            logger.info("SUCCESS ( "+ result +" ): ");
            logger.info("Query :" + query + " is executed successfully");

        }
        catch(Exception exp)
        {
            logger.warn(exp.getMessage() + " Function exec ", exp);
        }

        return result;
	}

}

