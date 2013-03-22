package com.agiledon.hadoop.example;

import com.agiledon.hadoop.example.domain.BookTag;
import com.agiledon.hadoop.example.domain.BookTags;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Zhang Yi
 * Date: 3/16/13
 * Time: 8:36 PM
 */
public class BookTagsRelevance extends Configured implements Tool {
    public static class BookMap extends Mapper<LongWritable, Text, Text, BookTag> {
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
                    int tagCount = Integer.parseInt(tags.get(tagItem).get("count").toString());
                    context.write(new Text(title), new BookTag(tagName, tagCount));
                }
            }
        }
    }

    public static class BookReduce extends Reducer<Text, BookTag, Text, BookTags> {
        public void reduce(Text key, Iterable<BookTag> values, Context context) throws IOException, InterruptedException {
            BookTags bookTags = new BookTags();
            for (BookTag tag : values) {
                bookTags.add(tag);
            }
            context.write(key, bookTags);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(BookTagsRelevance.class);
        job.setJobName("book relevance");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BookTag.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BookTags.class);

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
        int ret = ToolRunner.run(new BookTagsRelevance(), args);
        System.exit(ret);
    }
}
