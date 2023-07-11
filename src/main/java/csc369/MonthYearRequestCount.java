package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthYearRequestCount {

    public static final Class<Text> OUTPUT_KEY_CLASS = Text.class;
    public static final Class<IntWritable> OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text monthYear = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(" ");
            String timestamp = parts[3];
            String[] dateParts = timestamp.split("/");
            String month = dateParts[1];
            String year = dateParts[2].substring(0, 4);
            monthYear.set(month + "/" + year);
            context.write(monthYear, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<String, Integer> monthYearToCountMap;

        @Override
        protected void setup(Context context) {
            monthYearToCountMap = new TreeMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            monthYearToCountMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : monthYearToCountMap.entrySet()) {
                String monthYear = entry.getKey();
                int count = entry.getValue();
                context.write(new Text(monthYear), new IntWritable(count));
            }
        }
    }

}
