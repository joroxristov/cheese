package com.hadoop.demo.CheeseProject;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperOptionTwo extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

	String manufacturerProvCode; 
	String milkTypeEn;
	String categoryTypeEn;
	
	@Override
	public void configure(JobConf job) {
		this.manufacturerProvCode = job.get("manufacturerProvCode", "");
		this.milkTypeEn = job.get("units", "");
		this.categoryTypeEn = job.get("categoryTypeEn", "");
	}
	
	//columns[1] - manufacturerProvCode
	//columns[7] - categoryTypeEn
	//columns[8] - milkTypeEn
	//column[6] - organic
	
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
	        throws IOException {

	    String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	    
	    try {
	      if (checkLine(columns)) {
	        	int isOrganicValue = Integer.parseInt(columns[6].replaceAll("\"", ""));
	        	String outputKey = getOutputKey(columns);
	        	output.collect(new Text(outputKey), new IntWritable(isOrganicValue));
	    	}
	    } 
	    catch (Exception ex) {
	        System.err.println("Error on line: " + key);
	    }
	}

	private String getOutputKey(String[] columns) {
		return "Province: " + columns[1] + " Cheese category: " + columns[7]; 
	}

	private boolean checkLine(String[] columns) {
	    String manCodeFileValue = columns[1].replaceAll("\"", "").toLowerCase();
	    String categoryFileValue = columns[7].replaceAll("\"", "").toLowerCase();
	    String milkTypeFileValue = columns[8].replaceAll("\"", "").toLowerCase();
	    String isOrganicFileValue = columns[6].replaceAll("\"", "");

	    if(manCodeFileValue.isEmpty() || isOrganicFileValue.isEmpty()
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
