package com.myproj.hadoop;

import org.apache.hadoop.io.IntWritable;
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

    MapDriver<IntWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<IntWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
//        public void addAll(final List<Pair<K1, V1>> inputs) {
        mapDriver.withInput(new IntWritable(), new Text("Hallow world world"));
        List<Pair<Text, IntWritable>> outputRecords = new ArrayList<>();
        outputRecords.add(new Pair<>(new Text("Hallow"), new IntWritable(6)));
        outputRecords.add(new Pair<>(new Text("world"), new IntWritable(5)));
        outputRecords.add(new Pair<>(new Text("world"), new IntWritable(5)));
        mapDriver.withAllOutput(outputRecords);
        mapDriver.runTest();
    }

    @Test
    public void testMultipleMapper() throws IOException {
        List<Pair<IntWritable, Text>> inputs = new ArrayList<>();
        inputs.add(new Pair(new IntWritable(), new Text("Hallow world worl")));
        inputs.add(new Pair(new IntWritable(), new Text("a bb ccc")));
        inputs.add(new Pair(new IntWritable(), new Text("dddd eeeee ffffff")));
        mapDriver.addAll(inputs);
        List<Pair<Text, IntWritable>> outputRecords = new ArrayList<>();
        outputRecords.add(new Pair<>(new Text("Hallow"), new IntWritable(6)));
        outputRecords.add(new Pair<>(new Text("ccc"), new IntWritable(3)));
        outputRecords.add(new Pair<>(new Text("ffffff"), new IntWritable(6)));
        mapDriver.withAllOutput(outputRecords);
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(5));
        values1.add(new IntWritable(5));
        Pair<Text, List<IntWritable>> pair1 = new Pair<>(new Text("world"),values1);

        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(6));
        values2.add(new IntWritable(6));
        Pair<Text, List<IntWritable>> pair2 = new Pair<>(new Text("Hallow"),values2);

        List<Pair<Text, List<IntWritable>>> inputs = new ArrayList<>();
        inputs.add(pair1);
        inputs.add(pair2);

        reduceDriver.withAll(inputs);
        reduceDriver.withOutput(new Text("Hallow"), new IntWritable(6));
        reduceDriver.runTest();
    }

}