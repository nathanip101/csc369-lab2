package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLRequestCount {

    public static final Class<Text> OUTPUT_KEY_CLASS = Text.class;
    public static final Class<IntWritable> OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text url = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(" ");
            String path = parts[6];
            url.set(path);
            context.write(url, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, List<String>> countToURLMap;

        @Override
        protected void setup(Context context) {
            countToURLMap = new TreeMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            List<String> urls = countToURLMap.getOrDefault(sum, new ArrayList<>());
            urls.add(key.toString());
            countToURLMap.put(sum, urls);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, List<String>> entry : countToURLMap.entrySet()) {
                int count = entry.getKey();
                List<String> urls = entry.getValue();
                for (String url : urls) {
                    context.write(new Text(url), new IntWritable(count));
                }
            }
        }
    }

}
