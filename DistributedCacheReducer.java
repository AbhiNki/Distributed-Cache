import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistributedCacheReducer extends Reducer<Text ,IntWritable,Text,IntWritable> 
{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
    	int max = -9999;
    	for(IntWritable x: values)
        {
             if(x.get() > max )
             {
            	 max = x.get();
             }
        }
    	String col=colPositions(key, context);
    	context.write(new Text(col), new IntWritable(max));
    }
    
    public String colPositions(Text key, Context context) throws IOException
    {
    	Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	String[] header = null;
    	String columnName = null;
        try
        {
               BufferedReader readBuffer1 = new BufferedReader(new FileReader(uris[0].toString()));
               String line;
               while ((line=readBuffer1.readLine())!=null)
               {
                    header = line.split("\t");
               }
               readBuffer1.close(); 
        }       
        catch (Exception e)
        {
            System.out.println(e.toString());
        }
        for(int i = 0; i<header.length;i++)
        {
        	if(key.toString().equals(""+i))
        	{
        		columnName = header[i];
        	}
        }
		return columnName;
    	
    }
}