package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HTTPResponseCodeCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text responseCode = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(" ");
            String code = parts[8];
            responseCode.set(code);
            context.write(responseCode, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, StringBuilder> codeToCountMap;

        @Override
        protected void setup(Context context) {
            codeToCountMap = new TreeMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            StringBuilder codes = codeToCountMap.getOrDefault(sum, new StringBuilder());
            codes.append(key.toString()).append("\n");
            codeToCountMap.put(sum, codes);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, StringBuilder> entry : codeToCountMap.entrySet()) {
                int count = entry.getKey();
                String codes = entry.getValue().toString();
                String[] codeArray = codes.split("\n");
                for (String code : codeArray) {
                    context.write(new Text(code), new IntWritable(count));
                }
            }
        }
    }

}
