package csc369;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CalendarDayBytesCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable bytesSent = new IntWritable();
        private Text calendarDay = new Text();
        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("dd/MMM/yyyy");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(" ");
            try {
                Date date = inputDateFormat.parse(parts[3].substring(1, 12));
                calendarDay.set(inputDateFormat.format(date));
                int bytes = Integer.parseInt(parts[9]);
                bytesSent.set(bytes);
                context.write(calendarDay, bytesSent);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> bytesToDayMap;

        @Override
        protected void setup(Context context) {
            bytesToDayMap = new TreeMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            bytesToDayMap.put(sum, key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : bytesToDayMap.entrySet()) {
                int bytes = entry.getKey();
                String day = entry.getValue();
                context.write(new Text(day), new IntWritable(bytes));
            }
        }
    }

}
