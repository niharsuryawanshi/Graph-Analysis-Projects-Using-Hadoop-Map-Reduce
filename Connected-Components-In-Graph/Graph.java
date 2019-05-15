//Name: Nihar Suryawanshi
//UTA Id: 1001654583
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>();     // the vertex neighbors
    
    Vertex(){}
    Vertex (short tag1, long group1,long VID1, Vector<Long> adjacent1){
    
        this.tag=tag1;
        this.group=group1;
        this.VID=VID1;
        this.adjacent=adjacent1;
    }
    
    Vertex (short tag1,long group1){
        this.tag=tag1;
        this.group=group1;
    }
    
    public void readFields(DataInput in) throws IOException{
        tag= in.readShort();
        group= in.readLong();
        VID= in.readLong();
        
        adjacent = new Vector<Long>();
        int n = in.readInt();
        for(long i=0;i<n;i++){
            adjacent.add(in.readLong());
        }
        
        //Long cnt = in.readLong();
        //while(cnt-- >0){
            //adjacent.add(in.readLong());
        //}
        //adjacent= VectorWritable.readVector(in);
    }
    
    public void write (DataOutput out) throws IOException{
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        
        
        int n = adjacent.size();
        out.writeInt(n);
        
        for(int i=0;i<n;i++){
            out.writeLong(adjacent.get(i));
        }
    }
    
    public String toString(){
        return tag+" "+group+" "+VID+" "+adjacent;
    }
}


public class Graph {

    //First mapper
    public static class Mapper01 extends Mapper<Object,Text,LongWritable,Vertex>{
    @Override
    public void map (Object Key,Text value,Context context)throws IOException, InterruptedException{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            
            long vid = s.nextLong();
            Vector<Long> adj = new Vector<Long>();
            while(s.hasNext()){
                adj.add(s.nextLong());
            }
            
            short sn = 0;
            Vertex v = new Vertex(sn,vid,vid,adj);
            
            //System.out.println("Key="+vid +" -val= "+v);
            context.write(new LongWritable(vid),v);
        }
    }
    
    //First Reducer
    public static class Reducer01 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{
    public void reduce(LongWritable Key, Iterable<Vertex> values,Context context) throws IOException, InterruptedException{
            for(Vertex v : values){
            
                //System.out.println("Writting in Reduce 1");
                //System.out.println("Key = "+Key+"-val= "+v.tag+"- "+v.group+"- "+v.VID+"- "+v.adjacent);
                
                Vertex ver = new Vertex(v.tag,v.group,v.VID,v.adjacent);
                context.write(Key,ver);
            }
        }
    }
    //Second Mapper
    public static class Mapper02 extends Mapper<LongWritable,Vertex,LongWritable,Vertex>{
    //s@Override
    public void map(LongWritable Key,Vertex values,Context context )throws IOException, InterruptedException{
            
            //System.out.println("Mapper 02");
            //System.out.println("Writting context1 :Key="+values.VID+" -value "+values);
            
            context.write(new LongWritable(values.VID),values);
            
            for(Long v2 : values.adjacent){
                short sn = 1;
                Vertex vx = new Vertex(sn,values.group);
                
                //System.out.println("in Loop fo mapper 02");
                //System.out.println("Key="+v2+" -val"+vx);
                
                context.write(new LongWritable(v2),vx); 
            }
            
        }
    }
    //Second Reducer
    public static class Reducer02 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{
    
    public void reduce(LongWritable vid,Iterable<Vertex> values,Context context)throws IOException, InterruptedException {
            long m=Long.MAX_VALUE;
            
            Vector<Long> adj007 = new Vector<Long>();
        
            for(Vertex v : values){
                if(v.tag == 0){
                    adj007 = (Vector)v.adjacent.clone();
                }
                m = Math.min(m,v.group);
            }
            short sn = 0;
            long vid1= vid.get();
            
            Vertex ve = new Vertex(sn,m,vid1,adj007);
            //System.out.println("In Reducer 02: key= "+ m+" -val "+ve);
            context.write(new LongWritable(m),ve);
        }
    }
    
    //Third Mapper
    public static class Mapper03 extends Mapper<LongWritable,Vertex,LongWritable,IntWritable>{
    public void map(LongWritable group, Vertex values,Context context) throws IOException, InterruptedException{
    
            //System.out.println("In Mapper 03: key"+ group.get()+" -val "+"1");
            context.write(new LongWritable(group.get()),new IntWritable(1));
        }
    }
    //Third Reducer
    public static class Reducer03 extends Reducer<LongWritable, IntWritable,LongWritable,LongWritable>{
    public void reduce(LongWritable key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException{
            long m = 0;
            
            for(IntWritable v: values){
                //System.out.println("in Reducer 03: key= "+key+" -val "+v);
                m+=v.get(); 
            }
            
            context.write(new LongWritable(key.get()),new LongWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        //Job 1
        Job job = Job.getInstance();
        job.setJobName("nihar_mr01");
        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(Graph.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        
        job.setMapperClass(Mapper01.class);
        job.setReducerClass(Reducer01.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        job.waitForCompletion(true);
        
        //Job 2
        for ( short i = 0; i < 5; i++ ) {
        
            System.out.println("INSIDE FOR LOOP : ITERATION = "+i);
            
            Configuration conf = new Configuration();
            Job job2 = Job.getInstance(conf,"mpp");
            //Job job2 = Job.getInstance();
            //job2.setJobName("nihar_mp"+i);
              /* ... First Map-Reduce job to read the graph */
            job2.setJarByClass(Graph.class);
        
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
        
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
        
            job2.setMapperClass(Mapper02.class);
            job2.setReducerClass(Reducer02.class);
        
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
            SequenceFileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i));
            SequenceFileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
            job2.waitForCompletion(true);
            /* ... Second Map-Reduce job to propagate the group number */     
        }
        //Job 3
        Job job3 = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        job3.setJobName("nihar_mr03");
        /* ... First Map-Reduce job to read the graph */
        job3.setJarByClass(Graph.class);
        
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        
        job3.setMapperClass(Mapper03.class);
        job3.setReducerClass(Reducer03.class);
        
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        
        SequenceFileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
    }
}

