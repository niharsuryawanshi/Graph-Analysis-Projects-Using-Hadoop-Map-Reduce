//CSE 6331: Project 01
//Name: Nihar Suryawanshi


import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Graph {

	//First Mapper to get the no of neighbours
	public static class Mapper01 extends Mapper<Object,Text,LongWritable,LongWritable>{
		public void map(Object key,Text value,Context context)
				throws IOException,InterruptedException{
			
			Scanner sin = new Scanner(value.toString()).useDelimiter(",");
			long g = sin.nextLong();
			long n = sin.nextLong();
			context.write(new LongWritable(g),new LongWritable(n));
			sin.close();
		}
	}

	//Second Mapper to aggeregate for particular category
	public static class Mapper02 extends Mapper<LongWritable,LongWritable,LongWritable,LongWritable>{
	
		public void map(LongWritable Key,LongWritable value,Context context)
			throws IOException,InterruptedException{
			
			//Scanner sin = new Scanner(value.toString()).useDelimiter("\t");
			//double g = sin.nextDouble();
			//sin.next();
			//get the value
			//long cnt = sin.nextLong();
			
			context.write(value,new LongWritable(1));
			
		}

	}
	
	//First Reducer
	public static class Reduce01 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
		public void reduce(LongWritable key,Iterable<LongWritable> values,Context context)throws IOException,InterruptedException{
		
			long count = 0;
			for(LongWritable v:values){
				count++;
			}
			context.write(key,new LongWritable(count));
		}	
	}

	//Second Reducer
	public static class Reduce02 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
		public void reduce(LongWritable key,Iterable<LongWritable> values,Context context)throws IOException,InterruptedException{
		
			int sum = 0;
			for (LongWritable i : values){
				sum=sum+1;
			}
			context.write(key,new LongWritable(sum));
		}
	}

	
    public static void main ( String[] args ) throws Exception {
    	//config settings FOR MR JOB 1
    	Job job = Job.getInstance();
        job.setJobName("nihar_mp01");
        job.setJarByClass(Graph.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setMapperClass(Mapper01.class);
        job.setReducerClass(Reduce01.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path("/home/suryawan/project1/yureka007"));
        job.waitForCompletion(true);
        
        //config settings FOR MR JOB 2
        Job job2 = Job.getInstance();
        job2.setJobName("nihar_mp02");
        job2.setJarByClass(Graph.class);
        
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        
        job2.setMapperClass(Mapper02.class);
        job2.setReducerClass(Reduce02.class);
        
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job2,new Path("/home/suryawan/project1/yureka007"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);
        
   }

}
