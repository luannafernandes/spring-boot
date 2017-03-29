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

public class MyIntWritableTest {

    @Test
    public void testHappyCawe() throws IOException {
        MyIntWritable etalon = new MyIntWritable(12);
        MyIntWritable less = new MyIntWritable(11);
        MyIntWritable more = new MyIntWritable(13);
        MyIntWritable equal = new MyIntWritable(12);

        Assert.assertEquals(etalon.compareTo(less),-1);
        Assert.assertEquals(etalon.compareTo(more),1);
        Assert.assertEquals(etalon.compareTo(equal),0);
    }

//    @Test
//    public void testHappyCawe2() throws IOException {
//        IntWritable etalon = new IntWritable(12);
//        IntWritable less = new IntWritable(11);
//        IntWritable more = new IntWritable(13);
//        IntWritable equal = new IntWritable(12);
//
//        Assert.assertEquals(etalon.compareTo(less),1);
//        Assert.assertEquals(etalon.compareTo(more),-1);
//        Assert.assertEquals(etalon.compareTo(equal),0);
//    }
}