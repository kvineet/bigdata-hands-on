package in.kvineet.bigdata.ghcn_mapreduce.utils.input;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import in.kvineet.bigdata.ghcn_mapreduce.io.WeatherDataWritable;
import in.kvineet.bigdata.ghcn_mapreduce.utils.WeatherDataRecordReader;

public class WeatherDataInputFormat extends FileInputFormat<NullWritable, WeatherDataWritable> {

	@Override
	public RecordReader<NullWritable, WeatherDataWritable> createRecordReader(InputSplit input, TaskAttemptContext context)
			throws IOException, InterruptedException {
		context.setStatus(input.toString());
		return new WeatherDataRecordReader();
	}

}
