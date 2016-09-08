package in.kvineet.bigdata.ghcn_mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import in.kvineet.bigdata.ghcn_mapreduce.io.WeatherDataWritable;

public class GcdnReducer extends Reducer<Text, WeatherDataWritable, NullWritable, WeatherDataWritable>{

	private static WeatherDataWritable max=null;

	@Override
	protected void reduce(Text key, Iterable<WeatherDataWritable> value,
			Reducer<Text, WeatherDataWritable, NullWritable, WeatherDataWritable>.Context context) throws IOException, InterruptedException {
		for(WeatherDataWritable w : value){
			if(max==null)
				max=w;
			else if(max.compareTo(w)<0){
				max=w;
			}
		}
//		String strValue = max.getID()+"\t"+max.getDate()+"\t"+max.getValue();
		context.write(null, max);
	}
} 
