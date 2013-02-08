package com.esoteric.hadoop.sample;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroTryout extends Configured implements Tool {

	private static final Schema SCHEMA = new Schema.Parser().parse("{"
			+ "  \"namespace\": \"com.patentagility\","
			+ "  \"type\": \"record\"," + "  \"name\": \"document\","
			+ "  \"fields\": ["
			+ "    {\"name\": \"id\", \"type\": \"string\"},"
			+ "    {\"name\": \"id_aux\", \"type\": \"string\"},"
			+ "    {\"name\": \"import_timestamp\", \"type\": \"string\"},"
			+ "    {\"name\": \"contents\", \"type\": \"string\"}" + "  ]"
			+ "}");

	/**
	 *
	 */
	public static class MyAvroMapper extends
			Mapper<AvroKey<GenericData.Record>, NullWritable, Text, Text> {
		private Text key;
		private Text value;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Context context) {
			key = new Text("");
			value = new Text("");
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(AvroKey<GenericData.Record> record,
				NullWritable ignore, Context context) throws IOException,
				InterruptedException {
			GenericData.Record r = record.datum();
			
			key.set(r.get("id").toString());
			value.set(r.get("contents").toString());
			context.write(key, value);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf(), AvroTryout.class.getName());
		
	    job.setInputFormatClass(AvroKeyInputFormat.class);
	    AvroJob.setInputKeySchema(job, SCHEMA);
	    
	    job.setMapperClass(MyAvroMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	   
	    /* This job does not need any reducer */
	    job.setNumReduceTasks(0);
	    
	    return job.waitForCompletion(true) ? 1 : 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvroTryout(), args);
		System.exit(exitCode);
	}
}
