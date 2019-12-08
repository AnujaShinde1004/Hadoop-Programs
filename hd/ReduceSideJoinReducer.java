package com.deepak.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
	String join;
	String left;
	String right;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		join = conf.get("join");
		left = conf.get("left");
		right = conf.get("right");
	} 
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String leftRow = "";
		String rightRow = "";
		String row = "";
		for (Text value : values) {
			String data = value.toString();
			int index = data.indexOf("_");
			String prefix = data.substring(0, index);
			String line = data.substring(index + 1);
			if (prefix.equalsIgnoreCase(left)) {
				leftRow = line;deepak,10
			} else if (prefix.equalsIgnoreCase(right)) {
				rightRow = line;1,CSE
			}
		}

		if ("inner".equals(join)) {
			if (!"".equals(leftRow) && !"".equals(rightRow)) {
				row = key + "," + leftRow + "," + rightRow;
			}
		} else if ("leftouter".equals(join)) {
			if (!"".equals(leftRow)) {
				row = key + "," + leftRow + "," + rightRow;1,deepak,10,1,CSE
			}
		} else if ("rightouter".equals(join)) {
			if (!"".equals(rightRow)) {
				row = key + "," + leftRow + "," + rightRow;
			}
		} else if ("fullouter".equals(join)) {
			row = key + "," + leftRow + "," + rightRow;
		}

		if (!"".equals(row))
			context.write(new Text(row), NullWritable.get());
	}
}
