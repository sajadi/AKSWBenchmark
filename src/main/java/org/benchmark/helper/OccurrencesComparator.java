package org.benchmark.helper;

import java.util.Comparator;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Oct 12, 2010
 * Time: 1:50:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class OccurrencesComparator implements Comparator<String>{
    public int compare(String str1, String str2){
        String[] parts = str1.split("\t");
        String query1 = parts[0];
        int occurrences1 = Integer.parseInt(parts[1]);

        parts = str2.split("\t");
        String query2 = parts[0];
        int occurrences2 = Integer.parseInt(parts[1]);

        if(occurrences1 < occurrences2)
            return 1;
        else if(occurrences1 > occurrences2)
            return -1;
        else
            return 0;
    }
}