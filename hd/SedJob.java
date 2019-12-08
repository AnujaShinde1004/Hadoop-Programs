package com.deepak.hadoop;

import org.apache.hadoop.conf.Configuration;
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

public class SedJob implements Tool {

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
		Job sedjob = new Job(getConf());
		sedjob.setJobName("sed Count");
		sedjob.setJarByClass(this.getClass());
		sedjob.setMapperClass(SedMapper.class);
		sedjob.setNumReduceTasks(0);
		// grepJob.setMapOutputKeyClass(Text.class);
		// grepJob.setMapOutputValueClass(LongWritable.class);
		sedjob.setOutputKeyClass(Text.class);
		sedjob.setOutputValueClass(NullWritable.class);

		sedjob.setInputFormatClass(TextInputFormat.class);
		sedjob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(sedjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sedjob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);

		return sedjob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();

		conf1.set("sed-arg1", "hadoop");
		conf1.set("sed-arg2", "BigData");
		ToolRunner.run(conf1, new SedJob(), args);
	}

}
