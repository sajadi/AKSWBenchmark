package org.benchmark.main;

import org.apache.log4j.Logger;
import org.benchmark.helper.BenchmarkConfigReader;
import org.benchmark.ontology.OntologyClassInstancesLoader;
import org.benchmark.ontology.OntologyClassesSelector;
import org.dbpedia.extraction.ontology.OntologyClass;

import java.util.List;


/**
* Created by IntelliJ IDEA.
* User: Mohamed Morsey
* Date: Aug 11, 2010
* Time: 2:07:24 PM
* .
*/

public class OldMain {
    private static Logger logger = Logger.getLogger(OldMain.class);
    private static final int NUMBER_OF_THREADS = 8;
    private static Thread[] processingThreadList = new Thread[NUMBER_OF_THREADS];

    public static void main(String[] args) {
//        try{
//            BenchmarkConfigReader.readBenchmarkConfiguration();
//
//            FileInputStream fs= new FileInputStream("D:\\Leipzig University\\DBpediaExtraction\\SqlLog.sql");
//            /*JDBC jdbc = JDBC.getDefaultConnection();
//            StringBuffer str = new StringBuffer("");
//            byte []bArray  = new byte[17516];
//            fs.read(bArray);
//            String strValue = new String(bArray);
//            String []arrParameters = strValue.split("\n");
//
//
//            String sqlStmt = "INSERT INTO " + TABLENAME + "(" + FIELD_OAIID + ", " +
//                FIELD_RESOURCE + " , " + FIELD_JSON_BLOB + " ) VALUES ( ?, ? , ?  ) ";
//
//            PreparedStatement stmt = jdbc.prepare(sqlStmt,"");
//            jdbc.executeStatement(stmt, arrParameters);*/
//            String oaiid = "", uri = "", json = "";
//            ArrayList<Byte> arrBytes = new ArrayList<Byte>();
//            byte b;
//            while((b = (byte)fs.read()) != -1)
//            {
//                char ch = (char)b;
//                System.out.print(ch);
//                if(ch == '\n'){
//                    if(oaiid.equals("")){
//                        oaiid = new String(convertToByteArray(arrBytes));
//                    }
//                    else if(uri.equals("")){
//                        uri = new String(convertToByteArray(arrBytes));
//                    }
//                    else{
//                        json = new String(convertToByteArray(arrBytes));
//
//                        JDBC jdbc = JDBC.getDefaultConnection();
//                        String sqlStmt = "INSERT INTO " + TABLENAME + "(" + FIELD_OAIID + ", " +
//                        FIELD_RESOURCE + " , " + FIELD_JSON_BLOB + " ) VALUES ( ?, ? , ?  ) ";
//
//                        PreparedStatement stmt = jdbc.prepare(sqlStmt,"");
//                        jdbc.executeStatement(stmt, new String[]{oaiid, uri, json});
//
//                        oaiid = uri = json = "";
//                    }
//                    arrBytes = new ArrayList<Byte>();
//
//
//                }
//                else
//                    arrBytes.add(b);
//            }

//            int ch;
//            while((ch = fs.read()) !=- 1){
//                if(((char) ch ) != '\n')
//                    str.append((char)ch);
//                else{
//                    System.out.println(str.toString());
//                    String strValue = str.toString();
//                    strValue = strValue.replaceAll("\"","\\\"");
//                    PreparedStatement stmt = jdbc.prepare(strValue,"");
//                    jdbc.executeStatement(stmt, new String[0]);
//
//                    str = new StringBuffer("");
//                }
//            }
//        }
//        catch(Exception exp){
//
//        }
        
        BenchmarkConfigReader.readBenchmarkConfiguration();
        OntologyClassesSelector selector = new OntologyClassesSelector();
        OntologyClassInstancesLoader loader = new OntologyClassInstancesLoader();

        List<OntologyClass> ontologyClassList = selector.loadAllOntologyClasses();
        int TotalNumberOfInstances = 0;

        ///////////////////////////////////////////////////////////////////////////
        logger.info("Insertion to database started");
        loader.loadClassInstancesFromFile("D:/Testfile.txt");
        loader.insertInstancesIntoDatabase();
        logger.info("Insertion to database finished");
//        loader.selectInstancesFromDatabase();

        //Instantiate threads required to process the instances
//        for(int i=0; i<NUMBER_OF_THREADS; i++){
//            processingThreadList[i] = new InstanceProcessor("Thread" + i, 6);
//        }

        ///////////////////////////////////////////////////////////////////////////
        int numberofclasses = 0;
//        for(OntologyClass ontologyClass : ontologyClassList){
//            //This is not needed in the data generation process
////            loader.getSubsetOntologyClassInstances(ontologyClass);
//            System.out.println("Class is called " + ontologyClass);
//            ClassInstancesDesc desc = loader.loadClassInstancesFromDatabase(ontologyClass);
//
//            if(desc.getInstancesList().size()>0){
//                URI randomInstanceURI = desc.getRandomInstance();
//                while(randomInstanceURI != null){
//                    loader.constructOutputGraph(randomInstanceURI);
//                    randomInstanceURI = desc.getRandomInstance();
//                }
////            loader.constructOutputGraph(loader.getSubsetOntologyClassInstances(ontologyClass));
//                numberofclasses ++;
//            }
//
//
////            TotalNumberOfInstances += loader.getSubsetOntologyClassInstances(ontologyClass).size();
//        }
        logger.info("All instances are processed");
        ///////////////////////////////////////////////////////////////////////
//        try{
//            FileOutputStream fos = new FileOutputStream("D:/Testfile.txt");
//		    ObjectOutputStream out = new ObjectOutputStream(fos);
//            out.writeObject(loader.classInstancesArray);
//            out.close();
//            logger.info("Array successfully serialized");
//        }
//        catch (Exception exp){
//            logger.error(exp.getMessage());
//        }

        ///////////////////////////////////////////////////////////////////////


//        BenchmarkConfigReader.readBenchmarkConfiguration();
//
//        OntologyClassesSelector selector = new OntologyClassesSelector();
//        OntologyClassInstancesLoader loader = new OntologyClassInstancesLoader();
//
//        try{
//            List<OntologyClass> ontologyClassesList = selector.loadSubsetOntologyClasses();
//            int i=1;
//            int percentOfClasses = (int)BenchmarkConfigReader.percentageOfClassesConsidered ;
//            FileOutputStream classesFileStream = new FileOutputStream("./output/SelectedClasses_" + percentOfClasses + ".txt");
//            for(OntologyClass c: ontologyClassesList){
//                String strMessage = "Class number " + i++ + " is "+ c + "\r\n";
//                classesFileStream.write(strMessage.getBytes());
//               logger.info(strMessage);
//            }
//            classesFileStream.close();
//            for(OntologyClass ontologyClass: ontologyClassesList){
//                loader.constructOutputGraph(loader.getAllOntologyClassInstances(ontologyClass));
//            }
//
//            logger.info("The whole extraction process performed successfully");
//        }
//        catch(Exception exp){
//            logger.error("The extraction process failed due to "  + exp.getMessage());
//        }
        
    }

//    private static byte[] convertToByteArray(ArrayList<Byte> arrBytes) {
//        byte [] outputArrBytes = new byte[arrBytes.size()];
//        for(int i = 0; i<arrBytes.size(); i++)
//            outputArrBytes[i] = arrBytes.get(i);
//        return outputArrBytes;
//    }

}
