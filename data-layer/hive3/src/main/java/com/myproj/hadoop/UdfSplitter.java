package com.myproj.hadoop;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public class UdfSplitter extends UDF {

    public ArrayList<Text> evaluate(Text input) {
        Optional<Entry> entry = parseEntry(input.toString());
        if (entry.isPresent()) {
            return new ArrayList<>(Arrays.asList(new Text(entry.get().browser), new Text(entry.get().os)));
        }
        return null;
    }

//    static Optional<String> getIPinyou(String row) {
//        String[] items = row.split("\\t");
//        Optional<String> retVal = Optional.empty();
//        if (items.length >= 4) {
//            retVal = Optional.ofNullable(items[4]);
//        }
//        return retVal;
//    }

//    public static Optional<Entry> getEntry(String row) {
//        Optional<String> optional = getIPinyou(row);
//
//        Optional<Entry> retVal = Optional.empty();
//
//        if (optional.isPresent()) {
//            retVal = parseEntry(optional.get());
//        }
//        return retVal;
//    }

    public static Optional<Entry> parseEntry(String entryStrin) {
        Optional<Entry> retVal = Optional.empty();
        String[] items = entryStrin.split("\\s");
        if (items.length >= 2) {
            Entry entry = new Entry(items[0], items[1]);
            retVal = Optional.of(entry);
        }
        return retVal;
    }


    static class Entry {
        private String browser;
        private String os;

        public Entry(String item, String item1) {
            browser = item;
            os = item1;
        }

        public String getBrowser() {
            return browser;
        }

        public void setBrowser(String browser) {
            this.browser = browser;
        }

        public String getOs() {
            return os;
        }

        public void setOs(String os) {
            this.os = os;
        }
    }

}

