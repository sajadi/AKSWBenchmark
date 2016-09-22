package org.benchmark.helper;

/**
 * Created by IntelliJ IDEA.
 * User: Mohamed Morsey
 * Date: Oct 22, 2010
 * Time: 4:29:01 PM
 * This class contains some string utilities necessary for pre-processing the _query before feeding it the similarity
 * measure step 
 */
public class StringUtilities {
    private static String []keywordList = new String[]{"select", "from", "where", "limit", "offset",
        "ask", "construct", "describe", "optional", "filter", "distinct", "union", "_query="};

    /**
     * Searches for a string within another string with case ignored
     * @param mainString    The string to be searched
     * @param str   The string that should be found
     * @param fromIndex Starting index;
     * @return  The position at which the string is found, and -1 if it was not found
     */
    public static int ignoreCaseIndexOf ( String mainString, String str, int fromIndex ) {
        String s1 = mainString.toLowerCase (  ) ;
        String t1 = str.toLowerCase (  ) ;
        return s1.indexOf ( t1, fromIndex ) ;
    }

    public static String removeKeywordsFromQuery(String query){
        for(String keyword: keywordList){
            int keywordPos = 0;
            while(keywordPos >= 0){

                keywordPos = ignoreCaseIndexOf(query, keyword, keywordPos);
                if(keywordPos < 0)
                    break;

                query = query.substring(0, keywordPos) + query.substring(keywordPos + keyword.length());
            }

        }

        //We should remove all prefixes also, but the prefix should be removed by removing the keyword prefix along
        // with the prefix itself
        int prefixPos = 0;
        while(prefixPos >= 0){
            prefixPos = ignoreCaseIndexOf(query, "prefix", prefixPos);

            //all prefixes are already handled
            if(prefixPos < 0)
                break;


            //The position of the ending greater than sign, as the pref is of the form PREFIX  dc:   <http://purl.org/dc/elements/1.1/>
            //So the greater than sign is the end of the prefix
            int endingTagPos = ignoreCaseIndexOf(query , ">", prefixPos);
            if(endingTagPos < 0)
                break;
            query = query.substring(0, prefixPos) + query.substring((endingTagPos+1));
//            System.out.println(_query);
        }

        return query;
    }

//    private static
}
