package com.p7rox.esApi.utils;


import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@ToString
@Getter
public class QueryObject {

    protected int limit;
    protected int offset;
    protected String[] fieldList;
    protected String[] sortList;
    protected List<Map<String, String>> filterMap;
    protected String queryKey = null;
    protected String[] queryValue = null;
    protected boolean countonly;


    public QueryObject(final Map<String, String> params) throws Exception {
        String limitString = params.remove("limit");
        this.limit = (limitString == null) ? 0 : Integer.valueOf(limitString);

        String offsetString = params.remove("offset");
        this.offset = (offsetString == null) ? 0 : Integer.valueOf(offsetString);

        String fieldString = params.remove("fields");
        this.fieldList = (fieldString == null) ? null : fieldString.split(",");

        String sortString = params.remove("sort");
        this.sortList = (sortString == null) ? null : sortString.split("\\|");

        String countonlyString = params.remove("countonly");
        this.countonly = (countonlyString == null) ? false : Boolean.valueOf(countonlyString);

        if (!(this.sortList == null)) {
            throw new Exception("customised sort is not supported in this version");
        }

        String filterString = params.remove("filters");
        if (filterString != null) {
            this.filterMap = new ArrayList<Map<String, String>>();
            formatFilterMap(filterString);
        }


        // return bad request if there isn't exactly 1 query criteria
        if (params.size() > 1) {
            throw new Exception("More than 1 query criteria specified, not supported");
        } else if (params.size() == 1) {
            Iterator<Entry<String, String>> it = params.entrySet().iterator();

            Entry<String, String> pairs = it.next();
            this.queryKey = pairs.getKey();
            this.queryValue = preprocessIndexValue(pairs.getValue());
        }
    }

    public void formatFilterMap(final String filterString) {
        String str = filterString;
        int startIndex = str.lastIndexOf("(");
        int endIndex = str.indexOf(")", startIndex);
        if (startIndex > 0 && endIndex > 0) {
            String subFilter = str.substring(startIndex + 1, endIndex);
            // System.out.println("subFilter:" + subFilter);
            try {
                this.filterMap.add(processSingleLevelFilter(subFilter));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            str = str.replace("(" + subFilter + ")", "");
            // System.out.println("str:" + str);
            if (endIndex > 0) {
                formatFilterMap(str);
            }
        } else {
            try {
                this.filterMap.add(processSingleLevelFilter(filterString));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public Map<String, String> processSingleLevelFilter(final String filterString) throws Exception {

        String[] filterList = (filterString == null) ? null : filterString.split("\\|");

        if (filterList != null) {
            Map<String, String> singleFilterMap = new HashMap<String, String>();
            try {
                for (String filter : filterList) {
                    if (filter.trim().length() > 0) {
                        String[] pair = filter.split(":|\\]", 2);
                        if (singleFilterMap.containsKey(pair[0])) {
                            singleFilterMap.put(pair[0], singleFilterMap.get(pair[0]) + "||" + pair[1]);
                        } else {
                            singleFilterMap.put(pair[0], pair[1]);
                        }
                    }
                }
                return singleFilterMap;
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new Exception("Invalid filtering syntax");
            }
        } else {
            throw new Exception("Invalid filtering syntax");
        }

    }


    public static String[] preprocessIndexValue(final String input) {
        String[] valueArr = null;
        if (input != null) {
            String values = input.replace("\\'", "@@");
            valueArr = values.split(",(?=(?:[^\']*\'[^\']*\')*[^\']*$)");
            for (int i = 0; i < valueArr.length; i++) {
                if (valueArr[i].contains("'")) {
                    valueArr[i] = valueArr[i].replace("'", "");
                }
                valueArr[i] = valueArr[i].replace("@@", "'");
            }
        }
        return valueArr;
    }
}
