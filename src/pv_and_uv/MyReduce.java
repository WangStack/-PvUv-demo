package pv_and_uv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<DataBean, IntWritable, DataBean, NullWritable> {
	@Override
	protected void reduce(DataBean db, Iterable<IntWritable> intWritables, Context context) throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable intWritable : intWritables) {
			count+=intWritable.get();
		}
		db.setCount(count);
		context.write(db, NullWritable.get());
		
	}

}
