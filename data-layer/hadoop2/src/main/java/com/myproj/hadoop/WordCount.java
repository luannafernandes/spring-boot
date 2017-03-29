package com.myproj.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(WordCount.class);


    static String getIPinyou(String row) throws IOException {
        String iPinyou = null;
        Pattern pattern = Pattern.compile("^[\\w]+\\t\\d*\\t((?!null)\\w+)\\t");
        Matcher matcher = pattern.matcher(row);
        if (matcher.find()) {
            iPinyou = matcher.group(1);
        }
        return iPinyou;
    }

    public static class IntComparator extends WritableComparator {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IntComparator.class);

        public IntComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
//            LOGGER.info("compare " + l1 + " to " + l2);
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return (thisValue > thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TokenizerMapper.class);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String itr = getIPinyou(value.toString());
            if (itr != null) {
                context.write(new Text(itr), new IntWritable(1));
            } else {
//                LOGGER.info("doesnt fit requirement ");
            }
        }
    }

    static void printCounter(Reducer.Context context, TaskCounter taskCounter, org.slf4j.Logger LOGGER) {
        Counter counter = context.getCounter("org.apache.hadoop.mapred.Task$Counter", taskCounter.name());
        LOGGER.info(counter.getDisplayName() + counter.getValue());
    }

    static long getValue(Reducer.Context context, TaskCounter taskCounter) {
        Counter counter = context.getCounter("org.apache.hadoop.mapred.Task$Counter", taskCounter.name());
        return counter.getValue();
    }


    public static class Combiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Combiner.class);

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
//            printCounter(context,TaskCounter.COMBINE_INPUT_RECORDS,LOGGER);
//            printCounter(context,TaskCounter.COMBINE_OUTPUT_RECORDS,LOGGER);
            AtomicInteger ai = new AtomicInteger(0);
            values.forEach(intWritable -> ai.addAndGet(intWritable.get()));
            context.write(new Text(key), new IntWritable(ai.get()));
//            LOGGER.info("adding ... key = "+key+" val = "+ai.get());
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IntSumReducer.class);

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
//            printCounter(context,TaskCounter.REDUCE_INPUT_RECORDS,LOGGER);
//            printCounter(context,TaskCounter.REDUCE_OUTPUT_RECORDS,LOGGER);

            AtomicInteger ai = new AtomicInteger(0);
            values.forEach(intWritable -> ai.addAndGet(intWritable.get()));
//            if (getValue(context,TaskCounter.REDUCE_OUTPUT_RECORDS) < 5) {
            context.write(new Text(key), new IntWritable(ai.get()));
//            LOGGER.info("adding ... key = "+key+" val = "+ai.get());
//            }
        }
    }

    public static void main(String[] args) throws Exception {

//        if (true) return;
        System.out.println("input - " + args[0]);
        System.out.println("output - " + args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

//        job.setSortComparatorClass(IntComparator.class);
//        job.setGroupingComparatorClass(IntComparator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}