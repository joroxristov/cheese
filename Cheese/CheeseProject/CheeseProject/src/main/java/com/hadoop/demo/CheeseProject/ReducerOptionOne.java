package com.hadoop.demo.CheeseProject;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerOptionOne extends MapReduceBase implements Reducer <Text,DoubleWritable,Text,DoubleWritable>{

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		
		double sum = 0;
        long count = 0;

		while(values.hasNext())
		{
			sum += values.next().get();
            count = count + 1;
		}
		
		output.collect(key, new DoubleWritable(sum / count));

	}


}
