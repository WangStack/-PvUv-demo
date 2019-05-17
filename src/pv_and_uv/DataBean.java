package pv_and_uv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;

public class DataBean implements WritableComparable<DataBean>,Cloneable{
					 //日期            url          ip	 次数       
	public static Map<String, Map<String, Map<String,Integer>>> r1 = new HashMap<>();
	
	private String date;
	private String url;
	private String ip;
	private int pvCount;
	
	public DataBean(String date, String url, String ip) {
		this.date = date;
		this.url = url;
		this.ip = ip;
	}

	public DataBean() {}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		this.date = input.readUTF();
		this.url = input.readUTF();
		this.ip = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(date);
		output.writeUTF(url);
		output.writeUTF(ip);
	}

	@Override
	public int compareTo(DataBean o) {
		if(o.url.equals(this.url) && o.date.equals(this.date))
			return 0; 
		return 1;
	}
	
	@Override
	public String toString() {

		StringBuilder sbd = new StringBuilder();
		sbd.append(date + "\t" + url + "\t"  + "pv : " + pvCount + "  \t" + "uv : " );
		
		Iterator<Entry<String, Integer>> it = r1.get(date).get(url).entrySet().iterator();
		while(it.hasNext()){
			Entry<String, Integer> entry=it.next();
			sbd.append(entry.getKey() + "\t访问次数:" + entry.getValue());
			sbd.append(it.hasNext() ? "\r\n\t\t\t\t\t\t     " : "" );
		}
		
		return sbd.toString();
	}
	
	


	
	
	public int getPvCount() {
		return pvCount;
	}

	public void setCount(int count) {
		this.pvCount = count;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}







}
