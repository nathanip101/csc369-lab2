package csc369;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Hadoop");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 3) {
			System.out.println("Expected parameters: <job class> <input dir> <output dir>");
			System.exit(-1);
		} else if ("URL".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(URLRequestCount.ReducerImpl.class);
			job.setMapperClass(URLRequestCount.MapperImpl.class);
			job.setOutputKeyClass(URLRequestCount.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(URLRequestCount.OUTPUT_VALUE_CLASS);
		} else if ("HTTP".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(HTTPResponseCode.ReducerImpl.class);
			job.setMapperClass(HTTPResponseCode.MapperImpl.class);
			job.setOutputKeyClass(HTTPResponseCode.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(HTTPResponseCode.OUTPUT_VALUE_CLASS);
		} else if ("CountByBytes".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(CountByBytes.ReducerImpl.class);
			job.setMapperClass(CountByBytes.MapperImpl.class);
			job.setOutputKeyClass(CountByBytes.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(CountByBytes.OUTPUT_VALUE_CLASS);
		} else if ("ClientRequestCount".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(ClientRequestCount.ReducerImpl.class);
			job.setMapperClass(ClientRequestCount.MapperImpl.class);
			job.setOutputKeyClass(ClientRequestCount.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(ClientRequestCount.OUTPUT_VALUE_CLASS);
		} else if ("MonthYearRequestCount".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(MonthYearRequestCount.ReducerImpl.class);
			job.setMapperClass(MonthYearRequestCount.MapperImpl.class);
			job.setOutputKeyClass(MonthYearRequestCount.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(MonthYearRequestCount.OUTPUT_VALUE_CLASS);
		} else if ("CalendarDayBytesCount".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(CalendarDayBytesCount.ReducerImpl.class);
			job.setMapperClass(CalendarDayBytesCount.MapperImpl.class);
			job.setOutputKeyClass(CalendarDayBytesCount.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(CalendarDayBytesCount.OUTPUT_VALUE_CLASS);
		} else {
			System.out.println("Unrecognized job: " + otherArgs[0]);
			System.exit(-1);
		}

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
