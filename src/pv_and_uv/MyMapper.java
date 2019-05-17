package pv_and_uv;

import java.io.IOException;  
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, DataBean, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = replace(value.toString().split(","));
		
		DataBean db = new DataBean(values[0], values[1], values[2]);

	 	dataBean(values);
	
		context.write(db, new IntWritable(1));
	}

	private void dataBean(String[] values) {
		if(DataBean.r1.get(values[0]) != null){
			if( DataBean.r1.get(values[0]).get(values[1])!=null ) {
				if(DataBean.r1.get(values[0]).get(values[1]).get(values[2]) != null) {
					DataBean.r1.get(values[0]).get(values[1]).put(values[2], (DataBean.r1.get(values[0]).get(values[1]).get(values[2]).intValue()+1));
				} else {					
					DataBean.r1.get(values[0]).get(values[1]).put(values[2], 1);
				}
			} else {
				Map<String,Integer> ip = new HashMap<>();
				ip.put(values[2], 1);
				
				DataBean.r1.get(values[0]).put(values[1], ip);
			}
		} else {
			Map<String,Integer> ip = new HashMap<>();
			ip.put(values[2], 1);
			
			Map<String,Map<String,Integer>> url = new HashMap<>();
			url.put(values[1],ip);
			
			DataBean.r1.put(values[0],url);
		}
	}
	
	private String[] replace(String[] strs) {
		String[] strs2 = new String[strs.length];
		
		for (int i = 0; i < strs.length; i++) {
			strs2[i] = strs[i].replaceAll(" ", "");
		}
		return strs2;

	}
}
