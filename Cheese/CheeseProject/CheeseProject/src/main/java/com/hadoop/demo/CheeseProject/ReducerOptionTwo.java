package com.hadoop.demo.CheeseProject;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerOptionTwo extends MapReduceBase implements Reducer <Text,IntWritable,Text,DoubleWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		long allCount = 0;
        double organicCheeseCount = 0;

		while(values.hasNext())
		{
			allCount = allCount + 1;
		
	    	int isOrganic = values.next().get();
	    	if(isOrganic == 1) {
	    		organicCheeseCount = organicCheeseCount + 1;
	    	}
		    
		}
		
		output.collect(key, new DoubleWritable((organicCheeseCount / allCount)*100));
		
	}
	
	

	
}
