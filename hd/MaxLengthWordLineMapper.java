package com.deepak.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxLengthWordLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String maxWord = "";
		StringTokenizer st = new StringTokenizer(value.toString(), " ");
		while (st.hasMoreTokens()) {
			String nextToken = st.nextToken();
			if (nextToken.length() > maxWord.length()) {
				maxWord = nextToken;
			}
		}
		context.write(new Text(maxWord), new LongWritable(maxWord.length()));

	};

}
