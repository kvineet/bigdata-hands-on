package in.kvineet.bigdata.ghcn_pig_udf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import in.kvineet.bigdata.ghcn_pig_udf.io.WeatherDataWritable;

public class WeatherDataLoad extends LoadFunc {

	private RecordReader<LongWritable, Text> reader;
	private List<Tuple> values=null;
	
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
		if(values==null || values.isEmpty()){
			try{
				boolean notDone = reader.nextKeyValue();
				if(notDone){
					return null;
				}
				else{
					values = populateWeatherData(reader.getCurrentValue());
				}
			}
			catch(InterruptedException e){
				e.printStackTrace();
			}
		}
		else{
			tuple = values.remove(values.size()-1);
			return tuple;
		}
		return null;
	}
	
	private List<Tuple> populateWeatherData(Text value){
		String strValue = value.toString();
		DateFormat df = new SimpleDateFormat("yyyymmdd");
		List<Tuple> weatherList = new ArrayList<Tuple>(31);
		String strId 		= strValue.substring(0, 11);
		String strDate		= strValue.substring(11,17);
		String strElement 	= strValue.substring(17, 21);
		
		for(int i=0;i<31;i++){
			try {
				strDate = strDate + String.format("%02d", i+1);
				Integer measurement	= Integer.parseInt(strValue.substring((21+8*i), (26+8*i)).trim());
				String flags = strValue.substring((26+8*i), (29+8*i));
				List l = new ArrayList<Object>();
				l.add(strId);
				
				l.add(strDate);
				l.add(strElement);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return weatherList;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
		this.reader = reader;

	}

	@Override
	public void setLocation(String path, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, path);
	}

}
