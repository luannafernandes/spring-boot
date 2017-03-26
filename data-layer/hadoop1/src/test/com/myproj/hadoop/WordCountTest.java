package com.myproj.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountTest {

//    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
//    ReduceDriver<IntWritable, Text, Text, IntWritable> reduceDriver;
//    MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, IntWritable> mapReduceDriver;
//
//    @Before
//    public void setUp() {
//        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
//        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
//        mapDriver = MapDriver.newMapDriver(mapper);
//        reduceDriver = ReduceDriver.newReduceDriver(reducer);
//        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
//    }
//
//    @Test
//    public void testMultipleMapper() throws IOException {
//        List<Pair<LongWritable, Text>> inputs = new ArrayList<>();
//        inputs.add(new Pair<>(new LongWritable(), new Text("Hallow world worl")));
//        inputs.add(new Pair<>(new LongWritable(), new Text("a bb ccc")));
//        inputs.add(new Pair<>(new LongWritable(), new Text("dddd eeeee ffffffMax")));
//        mapDriver.addAll(inputs);
//        List<Pair<IntWritable,Text>> outputRecords = new ArrayList<>();
//        outputRecords.add(new Pair<>(new IntWritable(1),new Text("Hallow")));
//        outputRecords.add(new Pair<>(new IntWritable(1),new Text("ccc")));
//        outputRecords.add(new Pair<>(new IntWritable(1),new Text("ffffffMax")));
//        mapDriver.withAllOutput(outputRecords);
//        mapDriver.runTest();
//    }
//
//    @Test
//    public void testReducer() throws IOException {
////        TODO: code dublicate
//        List<Text> values1 = new ArrayList<Text>();
//        values1.add(new Text("Hallow"));
//        values1.add(new Text("ccc"));
//        values1.add(new Text("ffffffMax"));
//        Pair<IntWritable, List<Text>> pair1 = new Pair<>(new IntWritable(1),values1);
//
//        List<Pair<IntWritable, List<Text>>> inputs = new ArrayList<>();
//        inputs.add(pair1);
//
//        reduceDriver.withAll(inputs);
//        reduceDriver.withOutput(new Text("ffffffMax"), new IntWritable("ffffffMax".length()));
//        reduceDriver.runTest();
//    }

}