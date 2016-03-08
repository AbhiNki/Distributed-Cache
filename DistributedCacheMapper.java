import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedCacheMapper extends Mapper<Object, Text, Text, IntWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
    	
        String[] dataline = value.toString().split("\t");
        
        for(int i = 0; i<dataline.length;i++)
        {
        	context.write(new Text(""+i), new IntWritable(dataline[i].length()));
        }
    }
}
