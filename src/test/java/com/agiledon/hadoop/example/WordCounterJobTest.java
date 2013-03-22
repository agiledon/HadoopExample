package com.agiledon.hadoop.example;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Zhang Yi
 * Date: 3/12/13
 * Time: 12:57 PM
 */
public class WordCounterJobTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        WordCounterJob.Map tokenizerMapper = new WordCounterJob.Map();
        WordCounterJob.Reduce reducer = new WordCounterJob.Reduce();
        mapDriver = MapDriver.newMapDriver(tokenizerMapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void should_execute_tokenizer_map_job() throws IOException {
        mapDriver.withInput(new LongWritable(12), new Text("I am Bruce Bruce"));
        mapDriver.withOutput(new Text("I"), new IntWritable(1));
        mapDriver.withOutput(new Text("am"), new IntWritable(1));
        mapDriver.withOutput(new Text("Bruce"), new IntWritable(1));
        mapDriver.withOutput(new Text("Bruce"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void should_execute_reduce_job() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(3));

        reduceDriver.withInput(new Text("Bruce"), values);
        reduceDriver.withOutput(new Text("Bruce"), new IntWritable(4));
        reduceDriver.runTest();
    }
}
