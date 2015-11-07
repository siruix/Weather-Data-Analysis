package com.sirui.app.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class ReadWriteHBase {
	public static class ReadWriteHBaseMapper extends TableMapper<Text, IntWritable> {
		public static final byte[] CF = "cf".getBytes();
		public static final byte[] ATTR1 = "station_id".getBytes();
		private final IntWritable ONE = new IntWritable(1);
	   	private Text text = new Text();
	   	@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        	String val = new String(value.getValue(CF, ATTR1));
        	System.out.println(val);
          	text.set(val);     // we can only emit Writables...

          	context.write(text, ONE);
		}
	}

	public static class ReadWriteHBaseReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> { 
		public static final byte[] CF = "cf".getBytes();
		public static final byte[] COUNT = "count".getBytes();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
    			for (IntWritable val : values) {
    				i += val.get();
    			}
    			Put put = new Put(Bytes.toBytes(key.toString()));
    			put.add(CF, COUNT, Bytes.toBytes(i));
    			try {
    			    context.write(new ImmutableBytesWritable(key.getBytes()), put);
   // 				context.write(null, put);
    			} catch (InterruptedException e) {
    			    e.printStackTrace();
    			}
		}
	}	
	public static void main(String[] args) throws Exception {
		
		Configuration conf =  HBaseConfiguration.create();
		Job job = new Job(conf, "ReadWriteHBase");
		job.setJarByClass(ReadWriteHBase.class);
// add scan
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		TableMapReduceUtil.initTableMapperJob(
				"cosc6376-hw2-weather",      // input table
				scan,	          // Scan instance to control CF and attribute selection
				ReadWriteHBaseMapper.class,   // mapper class
				Text.class,	          // mapper output key
				IntWritable.class,	          // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				"cloudc30_station_id_count", 
				ReadWriteHBaseReducer.class, 
				job);
		
		
		job.setMapperClass(ReadWriteHBaseMapper.class);
		job.setReducerClass(ReadWriteHBaseReducer.class);
		
		job.setInputFormatClass(TableInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);

		job.setNumReduceTasks(1);
	
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path("/cosc6376/cloudc30/hw2/output"));
		boolean b = job.waitForCompletion(true);
		if (!b) {
		    throw new IOException("error with job!");
		}
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
