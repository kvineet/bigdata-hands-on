package in.kvineet.bigdata.ghcn_mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import in.kvineet.bigdata.ghcn_mapreduce.io.WeatherDataWritable;

public class GcdnMapper extends Mapper<NullWritable, WeatherDataWritable, Text, WeatherDataWritable>{

	@Override
	protected void map(NullWritable key, WeatherDataWritable value,
			Mapper<NullWritable, WeatherDataWritable, Text, WeatherDataWritable>.Context context)
			throws IOException, InterruptedException {
		if(value.getElement().equalsIgnoreCase("TMAX")){
			context.write(new Text(value.getID()), value);
		}
	}



}
