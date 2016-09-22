package org.benchmark.helper;

// filename: OldExternalSort.java

import org.apache.log4j.Logger;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Pattern;

/**
* Goal: offer a generic external-memory sorting program in Java.
*
* It must be :
*  - hackable (easy to adapt)
*  - scalable to large files
*  - sensibly efficient.
*
* This software is in the public domain.
*
* By (in alphabetical order)
*   Philippe Beaudoin,  Jon Elsas,  Christan Grant, Daniel Haran, Daniel Lemire,
*  April 2010
* originally posted at
*  http://www.daniel-lemire.com/blog/archives/2010/04/01/external-memory-sorting-in-java/
*/
public class ExternalSort {

    private static Logger logger = Logger.getLogger(ExternalSort.class);

	// we divide the file into small blocks. If the blocks
	// are too small, we shall create too many temporary files.
	// If they are too big, we shall be using too much memory.
	public static long estimateBestSizeOfBlocks(File filetobesorted) {
		long sizeoffile = filetobesorted.length();
		// we don't want to open up much more than 1024 temporary files, better run
		// out of memory first. (Even 1024 is stretching it.)
		final int MAXTEMPFILES = 1024;
		long blocksize = sizeoffile / MAXTEMPFILES ;
		// on the other hand, we don't want to create many temporary files
		// for naught. If blocksize is smaller than half the free memory, grow it.
		long freemem = Runtime.getRuntime().freeMemory();
		if( blocksize < freemem/2)
		    blocksize = freemem/2;
		else {
			if(blocksize >= freemem)
			  System.err.println("We expect to run out of memory. ");
		}
		return blocksize;
	}

	/**
	 * This will simply load the file by blocks of x rows, then
	 * sort them in-memory, and write the result to a bunch of
	 * temporary files that have to be merged later.
     * Modified by M.Morsey
     * I added a call to a function to normalize the _query, by renaming all _query variables, in order to make similar queries
     * that use different variables counted as same _query.
	 *
	 * @param file some flat  file
     * @param   cmp The comparator
     * @param   renameVariables Whether to rename the variables of the _query or not
	 * @return a list of temporary flat files
	 */
	public static List<File> sortInBatch(File file, Comparator<String> cmp, boolean renameVariables) throws IOException {

        List<File> files = new ArrayList<File>();
        BufferedReader fbr;
        long blocksize;
        if(renameVariables){
            //Normalize file variables at first
            String normalizedFilename = normalizeFileVariables(file);
            File normalizedFile = new File(normalizedFilename);
            fbr = new BufferedReader(new FileReader(normalizedFile));
            blocksize = estimateBestSizeOfBlocks(normalizedFile);// in bytes
        }
        else {
            fbr = new BufferedReader(new FileReader(file));
            blocksize = estimateBestSizeOfBlocks(file);// in bytes
        }

        
//		long blocksize = estimateBestSizeOfBlocks(file);// in bytes
		try{
			List<String> tmplist =  new ArrayList<String>();
			String line = "";
			try {
				while(line != null) {
					long currentblocksize = 0;// in bytes
					while((currentblocksize < blocksize)
					&&(   (line = fbr.readLine()) != null) ){ // as long as you have 2MB
                        line = eliminateUnnecessaryData(line);
						tmplist.add(line);
						currentblocksize += line.length() * 2; // java uses 16 bits per character?
					}
					files.add(sortAndSave(tmplist,cmp));
					tmplist.clear();
				}
			} catch(EOFException oef) {
				if(tmplist.size()>0) {
					files.add(sortAndSave(tmplist,cmp));
					tmplist.clear();
				}
			}
		} finally {
			fbr.close();
		}
		return files;
	}

    //Removes any unnecessary stuff at the beginning of the _query, so get only the SPARQL _query without any metadata, e.g.
    //the time at which the _query is executed
    private static String eliminateUnnecessaryData(String lineFromFile) {
        int endingQuotePos = lineFromFile.lastIndexOf("\"");
        int startingQuestionMarkPos = lineFromFile.indexOf("?");
        if((startingQuestionMarkPos == -1) || (endingQuotePos == -1))
            return lineFromFile;

        String actualSPARQLQuery = lineFromFile.substring(startingQuestionMarkPos+1, endingQuotePos);
        return actualSPARQLQuery;

        /*int endingQuotePos = lineFromFile.lastIndexOf("\"");
        int startingQuotePos = lineFromFile.lastIndexOf("\"", endingQuotePos-1);
        if((startingQuotePos == -1) || (endingQuotePos == -1))
            return lineFromFile;

        String actualSPARQLQuery = lineFromFile.substring(startingQuotePos+1, endingQuotePos);
        return actualSPARQLQuery;*/
    }


    public static File sortAndSave(List<String> tmplist, Comparator<String> cmp) throws IOException  {
		Collections.sort(tmplist,cmp);  //
		File newtmpfile = File.createTempFile("sortInBatch", "flatfile");
		newtmpfile.deleteOnExit();
		BufferedWriter fbw = new BufferedWriter(new FileWriter(newtmpfile));
		try {
			for(String r : tmplist) {
				fbw.write(r);
				fbw.newLine();
			}
		} finally {
			fbw.close();
		}
		return newtmpfile;
	}
	/**
	 * This merges a bunch of temporary flat files
	 * @param files
	 * @param output file
         * @return The number of lines sorted. (P. Beaudoin)
	 */
	public static int mergeSortedFiles(List<File> files, File outputfile, final Comparator<String> cmp) throws IOException {
		PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(11,
            new Comparator<BinaryFileBuffer>() {
              public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
                return cmp.compare(i.peek(), j.peek());
              }
            }
        );
		for (File f : files) {
			BinaryFileBuffer bfb = new BinaryFileBuffer(f);
			pq.add(bfb);
		}
		BufferedWriter fbw = new BufferedWriter(new FileWriter(outputfile));
		int rowcounter = 0;
		try {
			while(pq.size()>0) {
				BinaryFileBuffer bfb = pq.poll();
				String r = bfb.pop();
				fbw.write(r);
				fbw.newLine();
				++rowcounter;
				if(bfb.empty()) {
					bfb.fbr.close();
					bfb.originalfile.delete();// we don't need you anymore
				} else {
					pq.add(bfb); // add it back
				}
			}
		} finally {
			fbw.close();
			for(BinaryFileBuffer bfb : pq ) bfb.close();
		}
		return rowcounter;
	}

//	public static void main(String[] args) throws IOException {
//		if(args.length<2) {
//			System.out.println("please provide input and output file names");
//			return;
//		}
//		String inputfile = args[0];
//		String outputfile = args[1];
//		Comparator<String> comparator = new Comparator<String>() {
//			public int compare(String r1, String r2){
//				return r1.compareTo(r2);}};
//		List<File> l = sortInBatch(new File(inputfile), comparator) ;
//		mergeSortedFiles(l, new File(outputfile), comparator);
//	}

    /**
     * Counts the occurrence of each _query in the inFile and writes each _query along with its occurrences in the outFile
     * @param inFile    The input file
     * @param outFile   The output file
     */
    public static void countQueryOccurrencesInFile(File inFile, File outFile){
        FileReader inReader;
        FileWriter outWriter;
        LineNumberReader lnReader;
        String prevQuery = "";
        int prevQueryCountOfOccurrences = 0;
        String query = ""; 
        try{
            inReader = new FileReader(inFile);
            outWriter = new FileWriter(outFile);
            lnReader = new LineNumberReader(inReader);
            boolean firstQuery = true;
            try{
                while ((query = lnReader.readLine()) != null){
                    //That is the first _query to be read from the file, so we just set the prevQuery, and prevQueryCountOfOccurrences
                    if(firstQuery){
                        prevQuery = query;
                        prevQueryCountOfOccurrences++;
                        firstQuery = false;
                        continue;
                    }
                    //The previous _query occurs once more, so we should just increment the counter and proceed
                    if(query.compareTo(prevQuery) == 0){
                        prevQueryCountOfOccurrences ++;
                    }
                    //The _query is different from the previous one, so we should write the prevQuery, and prevQueryCountOfOccurrences
                    //to the output file, set prevQuery equal to _query and reset prevQueryCountOfOccurrences to 0
                    else{
                        outWriter.write(prevQuery + "\t" + prevQueryCountOfOccurrences + "\n");
                        prevQuery = query;
                        prevQueryCountOfOccurrences = 1;
                        logger.info("Query = " + prevQuery + " along with its number of occurrences is successfully written to file");
                    }
                }

                //Finalize and write the final _query and its number of occurrence
                outWriter.write(prevQuery + "\t" + prevQueryCountOfOccurrences + "\n");
                logger.info("Query = " + prevQuery + " along with its number of occurrences is successfully written to file");
            }
            finally{
                lnReader.close();
                outWriter.flush();
                outWriter.close();
            }
        }
        catch (Exception exp){
            logger.error("Failed to count queries in file " + inFile.getAbsolutePath(), exp);
        }
    }

    /**
     * Normalizes all queries that exist in the passed file by renaming all variables to sequential variable set, e.g.
     * var1, var2, var3, ....
     * So the queries that give the same results but use different variables names will be counted as same queries   
     * @param inFile    The file containing the queries
     */
    private static String normalizeFileVariables(File inFile) {
        String inputFilename = inFile.getAbsolutePath();

        //Replace the original extension of the file to lg2, as it will be used only by our application, and only
        //during the sorting process
        if(inputFilename.lastIndexOf(".")<0)
            throw new IndexOutOfBoundsException("File extension cannot be extracted from file name");

        String outputFilename = inputFilename.substring(0,inputFilename.lastIndexOf(".")) +  ".lg2";
        File outFile = new File(outputFilename);

        FileReader inReader;
        FileWriter outWriter;
        LineNumberReader lnReader;
        String query = "";
        try{
            inReader = new FileReader(inFile);
            outWriter = new FileWriter(outFile);
            lnReader = new LineNumberReader(inReader);

            //Read a _query from input file
            while ((query = lnReader.readLine()) != null){
                try{
//                    System.out.println("Query before elimiate = " + _query);
                    query = eliminateUnnecessaryData(query);
                    if((query == null) || (query.compareTo("") == 0))
                            continue;
//                    System.out.println("Query before = " + _query);
//                    _query = URLDecoder.decode(_query, "UTF-8");
//                    System.out.println(_query);
//                    System.out.println("Query before = " + URLDecoder.decode(_query, "UTF-8"));
                    query = normalizeQueryVariables(query);
//                    outWriter.write(URLEncoder.encode(_query, "UTF-8") + "\n");
                    outWriter.write(query + "\n");
//                    System.out.println(URLEncoder.encode(URLDecoder.decode(_query, "UTF-8"), "UTF-8"));
                }
                catch (Exception exp){
                    logger.error("Query " + query + " cannot be processed due to, " + exp.getMessage(), exp );
                }
            }
            inReader.close();

            outWriter.flush();
            outWriter.close();
            return outputFilename;
        }
        catch(Exception exp){
            logger.error("Normalized queries file cannot be generated due to, " + exp.getMessage(), exp);
            outFile.delete();//File should be deleted as there is a serious problem
            return "";
        }
    }

    /**
     * Normalizes the passed _query by renaming all variables to sequential variable set, e.g. var1, var2, var3
     * @param query The _query the should be normalized
     * @return  The normalized _query.
     */
    public static String normalizeQueryVariables(String query){
        //First we should split the _query using the "&" character, because this character separates the parts of the _query
        //as the other parts e.g. default-graph-uri = http%3A%2F%2Fdbpedia.org, should not be touched
        String ampersandPattern = "\\?|&";
        Pattern ampersandSplitter = Pattern.compile(ampersandPattern);
        String[] queryParts = ampersandSplitter.split(query);

        try{
            boolean queryPartFound = false;
            int queryPartIndex = 0;
            for(String part: queryParts){
                //The _query part, in the SPARQL _query always starts with the word _query.
                if(part.toLowerCase().startsWith("_query")){
                    queryPartFound = true;
                    break;
                }
                queryPartIndex ++;
            }

            //the _query string may not have a _query part so we cannot continue
            if(!queryPartFound)
                return query;

            //The _query part, should be URL decoded as it is originally URL decoded
            //As the _query is in the form _query=....., we should remove the first part after equal sign
            int equalPos = queryParts[queryPartIndex].indexOf("=");
            String withoutPrefixQuery = queryParts[queryPartIndex].substring(equalPos + 1);

            String plainQuery =  renameVariables(URLDecoder.decode(withoutPrefixQuery, "UTF-8"));
            queryParts[queryPartIndex] = "_query=" + URLEncoder.encode(plainQuery, "UTF-8");

            String finalQuery = "";
            //We should reassemble the parts of the _query
            for(String part: queryParts)
                finalQuery += part + "&";

            //We extract the whole string except tha last character, as it is and extra "&"
            finalQuery = finalQuery.substring(0, finalQuery.lastIndexOf("&"));
            return finalQuery;

        }
        catch (Exception exp){
            logger.error("Query " + query + " cannot be variable-normalized due to, " + exp.getMessage(), exp);
            return query;
        }

    }

    /**
     * Renames all variables of the _query part
     * @param queryPart 
     * @return
     */
    private static String renameVariables(String queryPart) {
        

        //The idea is to split the _query according to separators, and get a list of all parts, and then select the
        //list of variables only
        String whiteSpacesPattern = "[,\\s]+";//The whiteSpacesPattern used to split the _query by the separators
        Pattern whiteSpacesSplitter = Pattern.compile(whiteSpacesPattern);
        String[] parts = whiteSpacesSplitter.split(queryPart);

        int varCount = 0;

        for(String part: parts){

            //The part is a variable
            if(part.startsWith("?")){
                String oldVarName = part;

                int j = oldVarName.length()-1;

                //Here we check the end of the variable, since the variable may delimited from other parts of the
                //_query with { or ( or any other character that it is not a whitespace
                while(j >= 0 ){
                    char ch = oldVarName.charAt(j);

                    if ((ch >= 'a') && (ch <= 'z')) break; // lowercase
                    if ((ch >= 'A') && (ch <= 'Z')) break; // uppercase
                    if ((ch >= '0') && (ch <= '9')) break; // numeric
                    if(ch == '_')   break;
                    j--;
                }

                String actualVariableName = oldVarName.substring(0, j+1);

                queryPart = queryPart.replace(actualVariableName, "?var" + varCount++);
            }
        }
        return queryPart;
    }

    /**
    * Removes the the queries that did occur few times
     * @param   inFile  Input file
     * @param   outFile Output file
    * @param    leastNumberOfOccurrences   The number of occurrences, so only the queries that occur more than that number
    * will be included, and the other will be discarded
    */
    public static void removeLeastFrequentQueries(File inFile, File outFile, int leastNumberOfOccurrences){
        
        FileReader inReader;
        FileWriter outWriter;
        LineNumberReader lnReader;
        String query = "";
        try{
            inReader = new FileReader(inFile);
            outWriter = new FileWriter(outFile);
            lnReader = new LineNumberReader(inReader);

            //Read a _query from input file
            while ((query = lnReader.readLine()) != null){
                try{
                    int tabIndex = query.indexOf("\t");
                    int numberOfOccurrences = Integer.parseInt(query.substring(tabIndex+1));

                    //If the numberOfOccurrences is less than leastNumberOfOccurrences, we should not continue, as the
                    //file is already sorted so all upcoming queries will have less or equal number of occurrences
                    if(numberOfOccurrences < leastNumberOfOccurrences)
                        break;
                    outWriter.write(query + "\n");
                }
                catch (Exception exp){
                    logger.error("Query " + query + " cannot be written into file, due to " + exp.getMessage(), exp );
                }
            }
            inReader.close();

            outWriter.flush();
            outWriter.close();
        }
        catch(Exception exp){
            logger.error("Removal of least frequent queries failed, due to " + exp.getMessage(), exp);
        }
    }

}

class BinaryFileBuffer  {
	public static int BUFFERSIZE = 2048;
	public BufferedReader fbr;
	public File originalfile;
	private String cache;
	private boolean empty;

	public BinaryFileBuffer(File f) throws IOException {
		originalfile = f;
		fbr = new BufferedReader(new FileReader(f), BUFFERSIZE);
		reload();
	}

	public boolean empty() {
		return empty;
	}

	private void reload() throws IOException {
		try {
          if((this.cache = fbr.readLine()) == null){
            empty = true;
            cache = null;
          }
          else{
            empty = false;
          }
      } catch(EOFException oef) {
        empty = true;
        cache = null;
      }
	}

	public void close() throws IOException {
		fbr.close();
	}


	public String peek() {
		if(empty()) return null;
		return cache.toString();
	}
	public String pop() throws IOException {
	  String answer = peek();
		reload();
	  return answer;
	}



}