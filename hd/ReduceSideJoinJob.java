package com.deepak.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoinJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job joinJob = new Job(getConf());
		joinJob.setJobName("Reduce Side Join Job");
		joinJob.setJarByClass(this.getClass());

		joinJob.setMapperClass(ReduceSideJoinMapper.class);
		joinJob.setMapOutputKeyClass(Text.class);
		joinJob.setMapOutputValueClass(Text.class);

		joinJob.setReducerClass(ReduceSideJoinReducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(NullWritable.class);

		joinJob.setInputFormatClass(TextInputFormat.class);
		joinJob.setOutputFormatClass(TextOutputFormat.class);

		String multipath = args[0];
		for (int i = 1; i < args.length - 1; i++) {
			multipath = multipath + "," + args[i];
		}
		String output = args[args.length - 1];

		// setting the input file path
		// FileInputFormat.addInputPath(joinJob, new Path(args[0]));
		FileInputFormat.addInputPaths(joinJob, multipath);

		// setting the output folder path
		// FileOutputFormat.setOutputPath(joinJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(joinJob, new Path(output));

		// Path outputpath = new Path(args[1]);
		Path outputpath = new Path(output);
		outputpath.getFileSystem(getConf()).delete(outputpath, true);

		return joinJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("join", "leftouter");
		conf.set("left", "employee");
		conf.set("right", "info");

		ToolRunner.run(conf, new ReduceSideJoinJob(), args);
	}

}









