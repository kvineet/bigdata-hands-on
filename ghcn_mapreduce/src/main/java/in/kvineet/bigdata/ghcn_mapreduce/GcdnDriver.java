package in.kvineet.bigdata.ghcn_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import in.kvineet.bigdata.ghcn_mapreduce.io.WeatherDataWritable;
import in.kvineet.bigdata.ghcn_mapreduce.utils.input.WeatherDataInputFormat;

public class GcdnDriver extends Configured implements Tool {

	//static final String HOST_NAME="192.168.2.108";
	
	public int run(String[] args) throws Exception {
		if(args.length <2){
			System.out.println("Usage: hadoop -jar gcdn.jar <inputPath> <outputPath>"
					+ "\n\t\t\tOr\n"
					+ "java gcdn.jar -Jgcdn <inputPath> <outputPath>");
			System.exit(-1);
		}
		//String jarName=null;
		String infile, outfile;
		/*if(args[0].matches("-J(.)+")){
			jarName = args[0].substring(2, args[0].length());
			infile=args[1];
			outfile=args[2];
		}
		else{*/
			infile=args[0];
			outfile=args[1];
		//}
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://"+HOST_NAME+":9000/");
	
/*		FileSystem fs = FileSystem.get(conf);
		if(!fs.isFile(new Path(infile))){
			System.out.println("Input Path does not exists.");
			System.exit(-1);
		}
		if(fs.isDirectory(new Path(outfile))){
			System.out.println("Output directory already exists.");
			System.exit(-1);
		}*/
		//conf.set("yarn.resourcemanager.address", HOST_NAME+":8050");
		conf.set("mapreduce.framework.name", "yarn"); 
		/*conf.set("yarn.application.classpath",        
		             "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
		                + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
		                + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
		                + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*");
*/
		Job job = Job.getInstance(conf);
		/*if(jarName != null)
			job.setJar("target/"+jarName+".jar");
		else
		*/	job.setJarByClass(GcdnDriver.class);
		
		job.setJobName("ghcdn-analysis");
		FileInputFormat.addInputPath(job, new Path(infile));
		FileOutputFormat.setOutputPath(job, new Path(outfile));
		
		job.setInputFormatClass(WeatherDataInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(GcdnMapper.class);
		job.setReducerClass(GcdnReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WeatherDataWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(WeatherDataWritable.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

	public static void main(String[] args) {
		GcdnDriver gcdnDrv = new GcdnDriver();
		int exitCode;
		try {
			exitCode = ToolRunner.run(gcdnDrv,args);
			System.out.println("Exit Code: "+exitCode);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
