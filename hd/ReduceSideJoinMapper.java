package com.deepak.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	String prefix;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		InputSplit inputSplit = context.getInputSplit();
		FileSplit fileSplit = (FileSplit) inputSplit;
		Path path = fileSplit.getPath();
		String name = path.getName();
		prefix = name;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
		String inputLine = value.toString();

		int index = inputLine.indexOf(",");

		// join key
		String joinKey = inputLine.substring(0, index);

		// prefix_entire line except join key
		String exceptJoinKey = inputLine.substring(index + 1);
		String joinValue = prefix + "_" + exceptJoinKey;
		
		context.write(new Text(joinKey), new Text(joinValue));
	};
}