package org.benchmark.helper;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Oct 1, 2010
 * Time: 11:49:39 AM
 * Provides the basic utilities for file handling, such as decompressing files, iterating through files with certain
 * criteria
 */
public class FileUtilities {
    private static Logger logger = Logger.getLogger(FileUtilities.class);    

    /**
     * Decompresses a file with the passed filename, that was originally compressed in bz2 format
     * @param filename  The required filename
     */
    public static void decompressFile(String filename){
          try {
            // Open the compressed file
            BZip2CompressorInputStream in = new BZip2CompressorInputStream(new FileInputStream(filename));

            // Open the output file
            String target = filename.substring(0, filename.lastIndexOf("."));
            OutputStream out = new FileOutputStream(target);

            // Transfer bytes from the compressed file to the output file
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }

            // Close the file and stream
            in.close();
            out.close();
            logger.info("File " + filename + " is successfully decompressed");
         }

        catch (IOException exp) {
            logger.error("File " + filename + " cannot be decompressed due to " + exp.getMessage(), exp );            
        }

    }

    /**
     * Iterates though the files that exist within the specified root folder and with given extension list
     * @param   rootDirectory    The root directory from which we should start searching
     * @param extensions    The list of extensions that should be checked
     */
    public static Collection iterateThroughFiles(String rootDirectory, String[] extensions){
        try {

            //String[] extensions = {"bz2"};
            File root = new  File(rootDirectory);

            boolean recursive = true;

            //
            // Finds files within a root directory and optionally its
            // subdirectories which match an array of extensions. When the
            // extensions is null all files will be returned.
            //
            // This method will returns matched file as java.io.File
            //
            Collection files = FileUtils.listFiles(root, extensions, recursive);


            return files;
        }
        catch (Exception exp) {
            logger.error("Folder cannot be traversed due to " + exp.getMessage(), exp );
            return null;
        }
    }

    public static Collection iterateThroughFiles(String[] extensions){
        return iterateThroughFiles(BenchmarkConfigReader.triplesInputFoldername, extensions);
    }

    /**
     * Iterates through all compressed files and decompresses them
     */
    public static void decompressAllFiles(){
        Collection files = iterateThroughFiles(new String[]{"bz2"});

        for (Iterator iterator = files.iterator(); iterator.hasNext();) {
            File file = (File) iterator.next();
            if(file.getAbsolutePath().contains("nt"))
                decompressFile(file.getAbsolutePath());
        }
    }
}
