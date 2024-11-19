package com.hadoop.demo.CheeseProject;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperOptionOne extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable>{

	String manufacturerProvCode; 
	String milkTypeEn;
	String categoryTypeEn;
	
	@Override
	public void configure(JobConf job) {
		this.manufacturerProvCode = job.get("manufacturerProvCode", "");
		this.milkTypeEn = job.get("milkTypeEn", "");
		this.categoryTypeEn = job.get("categoryTypeEn", "");
	}
	
	//columns[1] - manufacturerProvCode
	//columns[3] - moisture_percent
	//columns[7] - categoryTypeEn
	//columns[8] - milkTypeEn
	
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
	        throws IOException {

	    String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	    
	    try {
	        if (checkLine(columns)) {
	      	double moisturePercent = Double.parseDouble(columns[3].replaceAll("\"", ""));
			String outputKey = getOutputKey(columns);
	        output.collect(new Text(outputKey), new DoubleWritable(moisturePercent));
	        	
	        }
	    } 
	    catch (Exception ex) {
	        System.err.println("Error on line: " + key);
	    }
	}

	private String getOutputKey(String[] columns) {
		return "Cheese category: " + columns[7] + " Milk type: " + columns[8]; 
	}

	private boolean checkLine(String[] columns) {
		String manCodeFileValue = columns[1].replaceAll("\"", "").toLowerCase();
	    String categoryFileValue = columns[7].replaceAll("\"", "").toLowerCase();
	    String milkTypeFileValue = columns[8].replaceAll("\"", "").toLowerCase();
	    String moisturePercentFileValue = columns[3].replaceAll("\"", "");

	    if(manCodeFileValue.isEmpty() || moisturePercentFileValue.isEmpty()
	    			|| categoryFileValue.isEmpty() || milkTypeFileValue.isEmpty()) {
	    	return false;
	    }
	    
	    if (!manufacturerProvCode.isEmpty() && !manCodeFileValue.contains(manufacturerProvCode.toLowerCase())) {
            return false;
        }
	    
	    if (!categoryTypeEn.isEmpty() && !categoryFileValue.contains(categoryTypeEn.toLowerCase())) {
            return false;
        }
	    
	    if (!milkTypeEn.isEmpty() && !milkTypeFileValue.contains(milkTypeEn.toLowerCase())) {
            return false;
        }

	    return true;
	}
}
