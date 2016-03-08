import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DistributedCacheDriver 
{
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception 
	{
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "Distributed Cache Example");
	    
	    job.setJarByClass(DistributedCacheDriver.class);
	    job.setMapperClass(DistributedCacheMapper.class);
	    //job.setCombinerClass(DistributedCacheReducer.class);
	    job.setReducerClass(DistributedCacheReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
	    DistributedCache.addCacheFile(new URI(args[1]), job.getConfiguration());
	    Path outputPath = new Path(args[2]);
	    
	     FileInputFormat.addInputPath(job, new Path(args[0]));
	     FileOutputFormat.setOutputPath(job, new Path(args[2]));

        outputPath.getFileSystem(conf).delete(outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	  }
	
    
	    	
	 
}
