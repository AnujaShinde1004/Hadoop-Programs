package com.deepak.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text key, LongWritable value, int noOfReducers) {
		// get the word in a lower case
		String word = key.toString().toLowerCase();
		
		if (word.length() == 0) {
			return 0;
		}
		// get the first character from word
		char firstChar = word.charAt(0);
		
		// get the partition number
		int diff = Math.abs(firstChar - 'a') % noOfReducers;
		
		//return the partition number
		return diff;
	}

}














