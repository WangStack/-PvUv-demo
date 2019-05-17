package pv_and_uv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Demo {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		String input = "file/pvuv.txt";
		String output="hdfs://192.168.20.10:9000/pvuv";
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Demo.class);
		job.setMapperClass(MyMapper.class);
		
		job.setMapOutputKeyClass(DataBean.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(input));
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(DataBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(MyPartition.class);
		job.setNumReduceTasks(2);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		
	}

}
