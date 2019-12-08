package com.deepak.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxLengthWordLineJob extends Configured implements Tool {

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Job maxLengthWordLineJob = new Job(getConf());
		maxLengthWordLineJob.setJobName(" Max Length Word ");
		maxLengthWordLineJob.setJarByClass(this.getClass());
		maxLengthWordLineJob.setMapperClass(MaxLengthWordLineMapper.class);
		maxLengthWordLineJob.setMapOutputKeyClass(Text.class);
		maxLengthWordLineJob.setMapOutputValueClass(LongWritable.class);

		maxLengthWordLineJob.setInputFormatClass(TextInputFormat.class);
		maxLengthWordLineJob.setOutputFormatClass(TextOutputFormat.class);
		maxLengthWordLineJob.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(maxLengthWordLineJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(maxLengthWordLineJob, new Path(args[1]));
		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		return maxLengthWordLineJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MaxLengthWordLineJob(), args);
	}

}
