package com.sirui.app.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReadHBase {
	public static class ReadHBaseMapper extends TableMapper<Text, Text> {
		public static final byte[] CF = "cf".getBytes();
		public static final byte[] ATTR1 = "value".getBytes();
		private Text toemit = new Text();
	   	private Text key = new Text();
	   	@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	   		String prcp_val = new String(value.getValue(CF, ATTR1));
	   		String val = new String(row.get()); 
	   		String[] tuple = val.split("-");
	   		String station_id = tuple[0];
	   		String date = tuple[1];
	   		String type = tuple[2];
	   		if (type.equals("PRCP")){
	   			key.set(station_id);
	   			toemit.set(date+","+prcp_val);
	   			context.write(key, toemit);
	   		}
		}
	}

	 public static class ReadHBaseReducer extends Reducer<Text, Text, Text, Text>  {
		 	@Override
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				StringBuilder sb = new StringBuilder();
				Map<String, String> unsortMap = new HashMap<String, String>();
		        
				for (Text val : values) {
					String[] duple = val.toString().split(",");
					unsortMap.put(duple[0], duple[1]); 
				}
				TreeMap<String,String> sorted_map = new TreeMap<String,String>(unsortMap);
				for (Map.Entry<String, String> entry : sorted_map.entrySet()) {
					sb.append(entry.getKey());
					sb.append(",");
		            sb.append(entry.getValue());
		            sb.append(";");
				}
				sb.deleteCharAt(sb.length()-1);
				context.write(key, new Text(sb.toString()));
			}
	}

	 
	public static void main(String[] args) throws Exception {
		
		Configuration conf =  HBaseConfiguration.create();
//		conf.writeXml(System.out);//debug
		Job job = new Job(conf, "ReadHBase");
		job.setJarByClass(ReadHBase.class);
// add scan
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		TableMapReduceUtil.initTableMapperJob(
				"cosc6376-hw2-weather",      // input table
				scan,	          // Scan instance to control CF and attribute selection
				ReadHBaseMapper.class,   // mapper class
				Text.class,	          // mapper output key
				Text.class,	          // mapper output value
				job);
		
		
		job.setMapperClass(ReadHBaseMapper.class);
		job.setReducerClass(ReadHBaseReducer.class);
		
		job.setInputFormatClass(TableInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/cosc6376/cloudc30/ReadHBase/output"));
		boolean b = job.waitForCompletion(true);
		if (!b) {
		    throw new IOException("error with job!");
		}
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
