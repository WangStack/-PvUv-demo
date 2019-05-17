package pv_and_uv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartition extends Partitioner<DataBean, IntWritable>{

	@Override
	public int getPartition(DataBean db, IntWritable arg1, int arg2) {
		if(db.getDate().equals("20150405"))
			return 0;
		else
			return 1;
	}



}
