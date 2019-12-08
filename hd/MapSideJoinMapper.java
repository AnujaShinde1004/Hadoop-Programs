package com.deepak.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private FSDataInputStream stream;

	@Override
	protected void setup(Context context) throws java.io.IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		URI[] uris = DistributedCache.getCacheFiles(conf);
		FileSystem fileSystem = FileSystem.get(uris[0], conf);
		stream = fileSystem.open(new Path(uris[0]));
	};

	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
		stream.seek(0);

		String inputLine = value.toString();
		int index = inputLine.indexOf(",");

		// join key
		String leftJoinKey = inputLine.substring(0, index);

		// entire line except join key
		String leftRow = inputLine.substring(index + 1);

		String dcLine;
		while ((dcLine = stream.readLine()) != null) {
			int dcindex = dcLine.indexOf(",");

			// join key
			String rightJoinKey = dcLine.substring(0, dcindex);

			// entire line except join key
			String rightRow = dcLine.substring(dcindex + 1);

			// inner join
			if (leftJoinKey.equals(rightJoinKey)) {
				String row = leftJoinKey + "," + leftRow + "," + rightRow;
				context.write(new Text(row), NullWritable.get());
				break;
			}
		}
	};

	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		stream.close();
	};
}






