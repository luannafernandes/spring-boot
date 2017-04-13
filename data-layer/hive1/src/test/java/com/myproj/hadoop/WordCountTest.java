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
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCountTest {

    public static final String ID = "Vh16OwT6OQNUXbj";
    public static final Integer QUANTITY = 12;

    String NORMAL = "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104008\t" +
            ID +
            "\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\tnull\n";
    String WITH_NULL = "a70cf453f7b670cd8de0b85446ab1bd6\t20130606000104025\tnull\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; .net clr 2.0.50727)\t121.236.218.*\t80\t85\t1\ttrqRTJubX5scFsf\t1443b15acc2356ef4e4dae19679cf5e6\t\tmm_14539978_2071324_8463350\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\tnull\n";

    String OUTP_OK = ID + " " + QUANTITY;
    String OUTP_NOT_OK = ID + " 1d2";


    MapDriver<LongWritable, Text, Text, IntWritable> mapDriverPiciker;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriverPicker;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriverPicker;

    MapDriver<LongWritable, Text, IntWritable, Text> mapDriverSorter;
    ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriverSorter;
//    MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, IntWritable> mapReduceDriverSorter;

    @Before
    public void setUp() {
        WordCount.QuantityPerIdMapper quantityPerIdMapper = new WordCount.QuantityPerIdMapper();
        WordCount.QuantityPerIdReducer QuantityPerIdReducer = new WordCount.QuantityPerIdReducer();
//        WordCount.Combiner combiner = new WordCount.Combiner();
//        combiner = ReduceDriver.newReduceDriver(combiner);
        mapDriverPiciker = MapDriver.newMapDriver(quantityPerIdMapper);
        reduceDriverPicker = ReduceDriver.newReduceDriver(QuantityPerIdReducer);
        mapReduceDriverPicker = MapReduceDriver.newMapReduceDriver(quantityPerIdMapper, QuantityPerIdReducer);

        WordCount.OrderByQuantityMapper orderByQuantityMapper = new WordCount.OrderByQuantityMapper();
        WordCount.OrderByQuantityReducer orderByQuantityReducer = new WordCount.OrderByQuantityReducer();
        mapDriverSorter = MapDriver.newMapDriver(orderByQuantityMapper);
        reduceDriverSorter = ReduceDriver.newReduceDriver(orderByQuantityReducer);
//        mapReduceDriverSorter = MapReduceDriver.newMapReduceDriver(quantityPerIdMapper, pickerReducer);
    }

    @Test
    public void testREGEXP() throws IOException {
        Assert.assertEquals(WordCount.getIPinyou(NORMAL), ID);
        Assert.assertNull(WordCount.getIPinyou(WITH_NULL));

        List<Integer> list = new ArrayList<>(Arrays.asList(new Integer[]{1, 2, 3}));

        AtomicInteger ai = new AtomicInteger(0);
        list.forEach(integer -> ai.incrementAndGet());
        Assert.assertEquals(ai.get(), 3);
    }

    @Test
    public void testFromOutput() throws IOException {
        Assert.assertEquals(WordCount.getFromFileOutput(OUTP_OK).getKey().get(), QUANTITY.intValue());
        Assert.assertEquals(WordCount.getFromFileOutput(OUTP_OK).getValue().toString(), ID);
        Assert.assertNull(WordCount.getFromFileOutput(OUTP_NOT_OK));
    }

    @Test
    public void testPickerMapper() throws IOException {
        List<Pair<LongWritable, Text>> inputs = new ArrayList<>();
        inputs.add(new Pair<>(new LongWritable(), new Text(NORMAL)));
        inputs.add(new Pair<>(new LongWritable(), new Text(WITH_NULL)));
        inputs.add(new Pair<>(new LongWritable(), new Text(NORMAL)));
        mapDriverPiciker.addAll(inputs);
        List<Pair<Text, IntWritable>> outputRecords = new ArrayList<>();
        outputRecords.add(new Pair<>(new Text(ID), new IntWritable(1)));
        outputRecords.add(new Pair<>(new Text(ID), new IntWritable(1)));
        mapDriverPiciker.withAllOutput(outputRecords);
        mapDriverPiciker.runTest();
    }

    //    @Ignore
    @Test
    public void testPickerReducer() throws IOException {
        List<IntWritable> values1 = new ArrayList<>();
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(2));
        Pair<Text, List<IntWritable>> pair1 = new Pair<>(new Text(ID), values1);

        List<Pair<Text, List<IntWritable>>> inputs = new ArrayList<>();
        inputs.add(pair1);

        reduceDriverPicker.withAll(inputs);
        reduceDriverPicker.withOutput(new Text(ID), new IntWritable(3));
        reduceDriverPicker.runTest();
    }

    @Test
    public void testSortMapper() throws IOException {
        List<Pair<LongWritable, Text>> inputs = new ArrayList<>();
        inputs.add(new Pair<>(new LongWritable(), new Text("hallow1 1")));
        inputs.add(new Pair<>(new LongWritable(), new Text("hallow2 2")));
        inputs.add(new Pair<>(new LongWritable(), new Text("hallow4 4")));
        inputs.add(new Pair<>(new LongWritable(), new Text("hallow3 2")));
        inputs.add(new Pair<>(new LongWritable(), new Text("hallow3 3d3")));
        mapDriverSorter.addAll(inputs);
        List<Pair<IntWritable, Text>> outputRecords = new ArrayList<>();
        outputRecords.add(new Pair<>(new IntWritable(1), new Text("hallow1")));
        outputRecords.add(new Pair<>(new IntWritable(2), new Text("hallow2")));
        outputRecords.add(new Pair<>(new IntWritable(4), new Text("hallow4")));
        outputRecords.add(new Pair<>(new IntWritable(2), new Text("hallow3")));
        mapDriverSorter.withAllOutput(outputRecords);
        mapDriverSorter.runTest();
    }

    @Test
    public void testSortReducer() throws IOException {
        List<Text> values1 = new ArrayList<>();
        values1.add(new Text("hallow1"));
        Pair<IntWritable, List<Text>> pair1 = new Pair<>(new IntWritable(1), values1);

        List<Text> values2 = new ArrayList<>();
        values1.add(new Text("hallow2"));
        values1.add(new Text("hallow3"));
        Pair<IntWritable, List<Text>> pair2 = new Pair<>(new IntWritable(2), values2);

        List<Text> values3 = new ArrayList<>();

        values1.add(new Text("hallow4"));
        Pair<IntWritable, List<Text>> pair3 = new Pair<>(new IntWritable(4), values3);

        List<Pair<IntWritable, List<Text>>> inputs = new ArrayList<>();
        inputs.add(pair1);
        inputs.add(pair2);
        inputs.add(pair3);

        reduceDriverSorter.withAll(inputs);

        List<Pair<IntWritable, Text>> outputRecords = new ArrayList<>();

        outputRecords.add(new Pair<>(new IntWritable(1), new Text("hallow1")));
        outputRecords.add(new Pair<>(new IntWritable(1), new Text("hallow2")));
        outputRecords.add(new Pair<>(new IntWritable(1), new Text("hallow3")));
        outputRecords.add(new Pair<>(new IntWritable(1), new Text("hallow4")));

        reduceDriverSorter.withAllOutput(outputRecords);
        reduceDriverSorter.runTest();
    }
}