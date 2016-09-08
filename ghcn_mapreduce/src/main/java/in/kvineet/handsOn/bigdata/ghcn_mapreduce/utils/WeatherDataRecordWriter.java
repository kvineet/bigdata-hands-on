package in.kvineet.bigdata.ghcn_mapreduce.utils;


import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import in.kvineet.bigdata.ghcn_mapreduce.io.WeatherDataWritable;

public class WeatherDataRecordWriter extends RecordWriter<NullWritable, WeatherDataWritable> {

	public WeatherDataRecordWriter(FSDataOutputStream out) {
		// TODO Auto-generated constructor stub
	}
	@Override
	public void write(NullWritable key, WeatherDataWritable value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
