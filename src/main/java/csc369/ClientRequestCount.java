package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ClientRequestCount {

    public static final Class<Text> OUTPUT_KEY_CLASS = Text.class;
    public static final Class<IntWritable> OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final String targetUrl = "/robots.txt";
        private final IntWritable one = new IntWritable(1);
        private Text client = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(" ");
            if (parts[6].equals(targetUrl)) {
                String clientAddress = parts[0];
                client.set(clientAddress);
                context.write(client, one);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, List<String>> countToClientMap;

        @Override
        protected void setup(Context context) {
            countToClientMap = new TreeMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            List<String> clients = countToClientMap.getOrDefault(sum, new ArrayList<>());
            clients.add(key.toString());
            countToClientMap.put(sum, clients);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, List<String>> entry : countToClientMap.entrySet()) {
                int count = entry.getKey();
                List<String> clients = entry.getValue();
                for (String client : clients) {
                    context.write(new Text(client), new IntWritable(count));
                }
            }
        }
    }

}
