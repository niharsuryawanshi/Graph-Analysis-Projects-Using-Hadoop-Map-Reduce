//Name: Nihar Suryawanshi
//CSE 6331
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Element implements Writable{
	short tag;
	int index;
	double value;
	
	Element(){}
	
	Element (short t, int i, double v){
		this.tag = t;
		this.index = i;
		this.value = v;
	}
	
	public void write ( DataOutput out ) throws IOException{
		out.writeShort(tag);
		out.writeInt(index);
		out.writeDouble(value);
	}
	
	public void readFields ( DataInput in ) throws IOException{
		tag = in.readShort();
		index = in.readInt();
		value = in.readDouble();
	}
	
	public String toString(){
		return tag+" "+index+" "+value;
	}
}

class Pair implements WritableComparable<Pair>{
	int i;
	int j;
	
	Pair(){}
	
	Pair (int i1, int j1){
		this.i = i1;
		this.j= j1;
	}
	
	public void write ( DataOutput out ) throws IOException{
		out.writeInt(i);
		out.writeInt(j);
	}
	
	public void readFields( DataInput in ) throws IOException{
		i = in.readInt();
		j= in.readInt();
	}
	
	public int compareTo (Pair o){
		
		if (i>o.i){ return 1;}
		else if(i<o.i){return -1;}
		else if(j>o.j){return 1;}
		else if(j<o.j){return -1;}
		
		return 0;
		//return(i==o.i) ? j-o.j : i-o.i ;
	}
	
	public String toString(){
		return i+" "+j;
	}
}	

public class Multiply {
	
	
	//Mapper for M matrix
	public static class Mapper01 extends Mapper<Object,Text,IntWritable,Element>{
	@Override
	public void map(Object Key, Text value,Context context) throws IOException, InterruptedException{
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i = s.nextInt();
			int j = s.nextInt();
			double val =  s.nextDouble();
			
			short tag = 0;
			
			Element elm = new Element(tag,i,val);
			
			context.write(new IntWritable(j),elm);
		}
	}
	
	//Mapper for N matrix
	public static class Mapper02 extends Mapper<Object,Text,IntWritable,Element>{
	@Override
	public void map(Object Key, Text value,Context context) throws IOException, InterruptedException{
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i = s.nextInt();
			int j = s.nextInt();
			double val =  s.nextDouble();
			
			short tag = 1;
			
			Element elm = new Element(tag,j,val);
			
			context.write(new IntWritable(i),elm);
		}
	}
	
	//Reducer 01 for intermediate output
	public static class Reducer01 extends Reducer<IntWritable,Element,Pair,DoubleWritable>{
	@Override
	public void reduce (IntWritable key,Iterable<Element> values, Context context) throws IOException,InterruptedException{
			Vector<Element> A = new Vector<Element>();
			Vector<Element> B = new Vector<Element>();
			
			for(Element v:values){
				if(v.tag == 0){
					A.add(new Element(v.tag,v.index,v.value));
				}
				else if (v.tag == 1){
					B.add(new Element(v.tag,v.index,v.value));
				}	
			}
			
			for(Element a:A){
				for(Element b:B){
					double d = a.value * b.value;
					context.write(new Pair(a.index,b.index),new DoubleWritable(d));
				}
			}
		}
	}
	
	//Mapper for MN matrix
	public static class Mapper03 extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable>{
	@Override
	public void map(Pair key, DoubleWritable value,Context context) throws IOException, InterruptedException{
			
			context.write(key,value);
		}
	}
	//Reducer for Final aggeregation
	public static class Reducer02 extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable>{
	@Override
	public void reduce (Pair key,Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException{
		
			double sum = 0.0;
			for (DoubleWritable v:values){
				
				sum += v.get();
			}
			context.write(key,new DoubleWritable(sum));
		}
	}
	
	
    public static void main ( String[] args ) throws Exception {
    	
    	//For MR job 1
    	Job job = Job.getInstance();
        job.setJobName("nihar_mp01");
        job.setJarByClass(Multiply.class);
        
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Element.class);
        
        job.setMapperClass(Mapper01.class);
        job.setMapperClass(Mapper02.class);
        job.setReducerClass(Reducer01.class);
        
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Mapper01.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Mapper02.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
        
        //For MR job 2
    	Job job2 = Job.getInstance();
        job2.setJobName("nihar_mp02");
        job2.setJarByClass(Multiply.class);
        
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        
        job2.setMapperClass(Mapper03.class);
        job2.setReducerClass(Reducer02.class);
        
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job2,new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
    }
}


