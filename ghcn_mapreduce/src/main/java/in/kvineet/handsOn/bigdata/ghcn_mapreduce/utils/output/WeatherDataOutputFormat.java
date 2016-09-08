package in.kvineet.bigdata.ghcn_mapreduce.utils.output;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import in.kvineet.bigdata.ghcn_mapreduce.io.WeatherDataWritable;
import in.kvineet.bigdata.ghcn_mapreduce.utils.WeatherDataRecordWriter;

public class WeatherDataOutputFormat extends FileOutputFormat<NullWritable, WeatherDataWritable> {

	@Override
	public RecordWriter<NullWritable, WeatherDataWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Path path = FileOutputFormat.getOutputPath(job);
		Path filePath = new Path(path,FileOutputFormat.getOutputName(job));
		FileSystem fs = path.getFileSystem(job.getConfiguration());
		FSDataOutputStream fsout = fs.create(filePath);
		
		return new WeatherDataRecordWriter(fsout);
	}

}
