package org.benchmark.helper;

import java.util.Comparator;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Oct 11, 2010
 * Time: 3:53:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class StringComparator implements Comparator<String>{
    public int compare(String str1, String str2){
        return str1.compareTo(str2);
    }
}
