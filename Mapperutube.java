package com.hadoopexpert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapperutube extends Mapper<LongWritable,Text,Text,IntWritable>{

	public void map(LongWritable key,Text value,Context context)
	{
			IntWritable one=new IntWritable(1);
			String s=value.toString();
			String str[]=s.split("\t");
			Text cat=new Text();
			if(str.length>5)
			{
				cat=new Text((str[3]));
			}
			try
			{
			context.write(cat, one);
			}
			catch(Exception e)
			{
				
			}
	}
}
