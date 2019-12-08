package com.deepak.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SedMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("key: " + key + " : value: " + value);
		if (value.toString().contains(context.getConfiguration().get("sed-arg1"))) {
			context.write(
					new Text(value.toString().replaceAll(context.getConfiguration().get("sed-arg1"),
							context.getConfiguration().get("sed-arg2"))), NullWritable.get());
		} else {
			context.write(value, NullWritable.get());
		}

	}
}
