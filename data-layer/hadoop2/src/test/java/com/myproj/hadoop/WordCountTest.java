package com.myproj.hadoop;

import junit.framework.Assert;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountTest {


    public static final String ID = "Vh16OwT6OQNUXbj";
    String NORMAL = "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104008\t" +
            ID +
            "\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\tnull\n";
    String WITH_NULL = "a70cf453f7b670cd8de0b85446ab1bd6\t20130606000104025\tnull\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; .net clr 2.0.50727)\t121.236.218.*\t80\t85\t1\ttrqRTJubX5scFsf\t1443b15acc2356ef4e4dae19679cf5e6\t\tmm_14539978_2071324_8463350\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\tnull\n";


    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<IntWritable,Text,IntWritable,Text > combiner;
    ReduceDriver<Text, IntWritable, Text, IntWritable > reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
//        WordCount.Combiner combiner = new WordCount.Combiner();
        mapDriver = MapDriver.newMapDriver(mapper);
//        combiner = ReduceDriver.newReduceDriver(combiner);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testREGEXP() throws IOException {
        Assert.assertEquals(WordCount.getIPinyou(NORMAL), ID);
        Assert.assertNull(WordCount.getIPinyou(WITH_NULL));

        List<Integer> list = new ArrayList<>(Arrays.asList(new Integer[]{1,2,3}));

        AtomicInteger ai = new AtomicInteger(0);
        list.forEach(integer -> ai.incrementAndGet());
        Assert.assertEquals(ai.get(),3);
    }

    @Test
    public void testMapper() throws IOException {
        List<Pair<LongWritable, Text>> inputs = new ArrayList<>();
        inputs.add(new Pair<>(new LongWritable(), new Text(NORMAL)));
        inputs.add(new Pair<>(new LongWritable(), new Text(WITH_NULL)));
        inputs.add(new Pair<>(new LongWritable(), new Text(NORMAL)));
        mapDriver.addAll(inputs);
        List<Pair<Text,IntWritable>> outputRecords = new ArrayList<>();
        outputRecords.add(new Pair<>(new Text(ID),new IntWritable(1)));
        outputRecords.add(new Pair<>(new Text(ID),new IntWritable(1)));
        mapDriver.withAllOutput(outputRecords);
        mapDriver.runTest();
    }

    @Ignore
    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values1 = new ArrayList<>();
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(2));
        Pair<Text, List<IntWritable>> pair1 = new Pair<>(new Text(ID),values1);

        List<Pair<Text, List<IntWritable>>> inputs = new ArrayList<>();
        inputs.add(pair1);

        reduceDriver.withAll(inputs);
        reduceDriver.withOutput( new Text(ID),new IntWritable(3));
        reduceDriver.runTest();
    }

}