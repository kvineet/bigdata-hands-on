package in.kvineet.bigdata.ghcn_mapreduce.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import in.kvineet.bigdata.ghcn_mapreduce.io.WeatherDataWritable;

public class WeatherDataRecordReader extends RecordReader<NullWritable,WeatherDataWritable> {

	private LineRecordReader ln = null;
	private WeatherDataWritable value = null;
	private List<WeatherDataWritable> values=null;
	
	@Override
	public void close() throws IOException {
		ln.close();
		
	}
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// return Nothing.
		return null;
	}
	@Override
	public WeatherDataWritable getCurrentValue() throws IOException, InterruptedException {
		// return current value. calling nextKeyValue() will set next object to value. 
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		ln = new LineRecordReader();
		ln.initialize(genericSplit, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(values==null || values.isEmpty()){
			//read new line
			if (!ln.nextKeyValue()){							// all lines read from the split file
				return false;									// no more records available
			}
			else{
				//LongWritable key = ln.getCurrentKey();		//We can safely ignore keys
				values = populateWeatherData(ln.getCurrentValue());		//lineRecordReader will return text data of 270 length.
				value = values.remove(values.size()-1);
				return true;
			}
		}
		else{
			//return already created object from list.
			value = values.remove(values.size()-1);
			return true;
		}
	}
	
	private List<WeatherDataWritable> populateWeatherData(Text value){
		String strValue = value.toString();
		List<WeatherDataWritable> weatherList = new ArrayList<WeatherDataWritable>(31);
		WeatherDataWritable wd = new WeatherDataWritable();
		wd.setID(strValue.substring(0, 11));
		String yyyymm = strValue.substring(11,17);
		wd.setElement(strValue.substring(17, 21));
		WeatherDataWritable w;
		for(int i=0;i<31;i++){
			try {
				w = wd.clone();
				w.setDate(yyyymm+String.format("%02d", i+1));
				w.setValue(Integer.parseInt(strValue.substring((21+8*i), (26+8*i)).trim()));
				w.setMflag(strValue.charAt(26+8*i));
				w.setQflag(strValue.charAt(27+8*i));
				w.setSflag(strValue.charAt(28+8*i));
				weatherList.add(w);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return weatherList;
	}
}
