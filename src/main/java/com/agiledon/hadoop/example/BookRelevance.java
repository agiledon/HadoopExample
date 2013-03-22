package com.agiledon.hadoop.example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: Zhang Yi
 * Date: 3/12/13
 * Time: 12:28 PM
 */
public class BookRelevance extends Configured implements Tool {
    public static class BookMap extends Mapper<LongWritable, Text, Text, Text> {
        private final static ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map map = objectMapper.readValue(value.toString(), Map.class);
            ArrayList<Map<String, Map<String, Map>>> arrayList = new ArrayList<Map<String, Map<String, Map>>>(map.values());

            for (int bookItem = 0; bookItem < arrayList.size(); bookItem++) {
                Map book = arrayList.get(bookItem).get("book");
                String title = book.get("title").toString();
                List<Map> tags = (ArrayList<Map>) book.get("tags");
                for (int tagItem = 0; tagItem < tags.size(); tagItem++) {
                    String tagName = tags.get(tagItem).get("name").toString();
                    context.write(new Text(title),new Text(tagName));
                }
            }
        }

    }

    public static class BookReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder tags = new StringBuilder();
            for (Text val : values) {
                if (isRedundantTag(tags, val)) {
                    continue;
                }
                tags.append(val.toString());
                tags.append("|");
            }
            context.write(key, new Text(tags.toString()));
        }

        private boolean isRedundantTag(StringBuilder tags, Text val) {
            return tags.toString().contains(val.toString());
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(BookRelevance.class);
        job.setJobName("book relevance");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(BookMap.class);
        job.setReducerClass(BookReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        outputDir.getFileSystem(getConf()).delete(outputDir, true);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new BookRelevance(), args);
        System.exit(ret);
    }

}