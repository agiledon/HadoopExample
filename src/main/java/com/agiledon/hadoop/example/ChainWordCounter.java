package com.agiledon.hadoop.example;

/**
 * Created with IntelliJ IDEA.
 * User: Zhang Yi
 * Date: 3/11/13
 * Time: 10:12 PM
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.*;


public class ChainWordCounter extends Configured implements Tool {
    public static class Tokenizer extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class UpperCaser extends MapReduceBase implements Mapper<Text, IntWritable, Text, IntWritable> {
        public void map(Text key, IntWritable count, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
            collector.collect(new Text(key.toString().toUpperCase()), count);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }

            result.set(sum);
            collector.collect(key, result);
        }
    }

    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), ChainWordCounter.class);
        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(jobConf, outputDir);
        outputDir.getFileSystem(getConf()).delete(outputDir, true);

        JobConf tokenizerMapConf = new JobConf(false);
        ChainMapper.addMapper(jobConf, Tokenizer.class, LongWritable.class, Text.class, Text.class, IntWritable.class, true, tokenizerMapConf);

        JobConf upperCaserMapConf = new JobConf(false);
        ChainMapper.addMapper(jobConf, UpperCaser.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, upperCaserMapConf);

        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(jobConf, Reduce.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, reduceConf);

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new ChainWordCounter(), args);
        System.exit(ret);
    }
}
