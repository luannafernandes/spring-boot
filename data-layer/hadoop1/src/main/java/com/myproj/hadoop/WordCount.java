package com.myproj.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text,  IntWritable, Text>{

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());

            String maxWord = "";

            while (itr.hasMoreTokens()) {
                String nextWorld = itr.nextToken();
                if(maxWord.length() < nextWorld.length()){
                    maxWord=nextWorld;
                }
            }
            context.write(new IntWritable(1), new Text(maxWord));
        }

    }

    public static class IntSumReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
// TODO: code dublicate

            System.out.println("asdf asdf asd !!!!!!!!"+key.toString());
            String maxWord = "";
            for (Text val : values) {
                if(maxWord.length() < val.toString().length()){
                    maxWord=val.toString();
                }
            }
//            context.write(new Text(maxWord), new IntWritable(maxWord.length()));
            context.write(new IntWritable(maxWord.length()),new Text(maxWord));
//
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

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}