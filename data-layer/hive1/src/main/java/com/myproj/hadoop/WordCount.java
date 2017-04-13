package com.myproj.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(WordCount.class);
    private static final int LIMIT_OF_OUTPUT_RECORDS = 100;

    static String getIPinyou(String row) throws IOException {
        String iPinyou = null;
        Pattern pattern = Pattern.compile("^[\\w]+\\t\\d*\\t((?!null)\\w+)\\t");
        Matcher matcher = pattern.matcher(row);
        if (matcher.find()) {
            iPinyou = matcher.group(1);
        }
        return iPinyou;
    }

    static Entry<IntWritable, Text> getFromFileOutput(String row) throws IOException {
        Entry<IntWritable, Text> entry = null;
        Pattern pattern = Pattern.compile("^([\\w]+)\\s+(\\d+)$");
        Matcher matcher = pattern.matcher(row);
        if (matcher.find()) {
            entry = new Entry<>(new IntWritable(Integer.parseInt(matcher.group(2))), new Text(matcher.group(1)));
        }
        return entry;
    }

    public static class IntDescComparator extends WritableComparator {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IntDescComparator.class);

        public IntDescComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return (thisValue > thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }

    public static class QuantityPerIdMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QuantityPerIdMapper.class);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String itr = getIPinyou(value.toString());
            if (itr != null) {
                context.write(new Text(itr), new IntWritable(1));
            }
        }
    }

    public static class OrderByQuantityMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OrderByQuantityMapper.class);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            Entry<IntWritable, Text> entry = getFromFileOutput(value.toString());
            if (entry != null) {
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    //    static void printCounter(Reducer.Context context, TaskCounter taskCounter, org.slf4j.Logger LOGGER) {
//        Counter counter = context.getCounter("org.apache.hadoop.mapred.Task$Counter", taskCounter.name());
//        LOGGER.info(counter.getDisplayName() + counter.getValue());
//    }
//
    static long getValue(Reducer.Context context, TaskCounter taskCounter) {
        Counter counter = context.getCounter("org.apache.hadoop.mapred.Task$Counter", taskCounter.name());
        return counter.getValue();
    }

    public static class QuantityPerIdReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QuantityPerIdReducer.class);

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            AtomicInteger ai = new AtomicInteger(0);
            values.forEach(intWritable -> ai.addAndGet(intWritable.get()));
            context.write(new Text(key), new IntWritable(ai.get()));
        }
    }

    public static class OrderByQuantityReducer
            extends Reducer<IntWritable, Text, IntWritable,  Text> {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QuantityPerIdReducer.class);

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (getValue(context, TaskCounter.REDUCE_OUTPUT_RECORDS) < LIMIT_OF_OUTPUT_RECORDS) {
                values.forEach(text -> {
                    try {
                        context.write(key, text);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

            }
        }
    }

    public static void main(String[] args) throws Exception {

        String input = args[0];
        String temp = args[1];
        String output = args[2];

        ControlledJob mainJob = new ControlledJob(getMainJob(input, temp), null);
        ControlledJob sortJob = new ControlledJob(getSortJob(temp, output), null);

        JobControl jbcntrl = new JobControl("jbcntrl");
        jbcntrl.addJob(mainJob);
        jbcntrl.addJob(sortJob);
        sortJob.addDependingJob(mainJob);
//        jbcntrl.run();
        new Thread(jbcntrl).start();
    }

    private static Job getSortJob(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Output 100 by quantity DESC");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(OrderByQuantityMapper.class);
        job.setCombinerClass(OrderByQuantityReducer.class);
        job.setReducerClass(OrderByQuantityReducer.class);
        job.setSortComparatorClass(IntDescComparator.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileSystem.get(conf).delete(new Path(output), true);
        FileOutputFormat.setOutputPath(job, new Path(output));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return job;
    }

    private static Job getMainJob(String input, String temp) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Per ID");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(QuantityPerIdMapper.class);
        job.setCombinerClass(QuantityPerIdReducer.class);
        job.setReducerClass(QuantityPerIdReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileSystem.get(conf).delete(new Path(temp), true);
        FileOutputFormat.setOutputPath(job, new Path(temp));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return job;
    }

    public static class Entry<K, V> {
        public K key;
        public V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }


}

