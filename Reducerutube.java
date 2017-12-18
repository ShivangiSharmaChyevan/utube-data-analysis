package com.hadoopexpert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducerutube extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	public void reduce(Text key,Iterable<IntWritable> values,Context context)
	{
		int tot=0;
		
		for(IntWritable i:values)
		{
			int i1=i.get();
			tot+=i1;
		}
		try
		{
		context.write(key, new IntWritable(tot));
		}
		catch(Exception e)
		{
			
		}
	}

}
