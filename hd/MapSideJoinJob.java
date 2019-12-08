package com.deepak.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

public class MapSideJoinJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job joinJob = new Job(getConf());
		joinJob.setJobName("Map Side Join Job");
		joinJob.setJarByClass(this.getClass());

		joinJob.setNumReduceTasks(0);

		joinJob.setMapperClass(MapSideJoinMapper.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(NullWritable.class);

		joinJob.setInputFormatClass(TextInputFormat.class);
		joinJob.setOutputFormatClass(TextOutputFormat.class);

		Path dcFile = new Path(args[1]);
		URI[] uris = new URI[1];
		uris[0] = dcFile.toUri();

		DistributedCache.setCacheFiles(uris, joinJob.getConfiguration());

		FileInputFormat.setInputPaths(joinJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(joinJob, new Path(args[2]));

		Path outputpath = new Path(args[2]);
		outputpath.getFileSystem(getConf()).delete(outputpath, true);

		return joinJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MapSideJoinJob(), args);
	}

}
