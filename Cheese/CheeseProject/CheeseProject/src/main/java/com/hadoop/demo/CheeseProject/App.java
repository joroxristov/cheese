package com.hadoop.demo.CheeseProject;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;


public class App extends JFrame {
	private String results;
	
	public static void main(String[] args) {
    	 App form = new App(); 	 
    }
	
    public App() {
    	init();
    }
	
    private void init() {
        setSize(600, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout());

        Font labelFont = new Font("Arial", Font.BOLD, 15);
        Font fieldFont = new Font("Arial", Font.PLAIN, 15);

        JPanel filterPanel = new JPanel(new GridLayout(6, 2, 10, 10));
        filterPanel.setBorder(BorderFactory.createTitledBorder(
            BorderFactory.createLineBorder(Color.BLUE, 2), 
            "Filters", 
            0, 
            0, 
            labelFont, 
            Color.BLUE
        ));
        
      
        ////////MANUFACTURER///////////////////////////////////
        JLabel manufacturerLabel = new JLabel("Manufacturer Prov Code:");
        manufacturerLabel.setFont(labelFont);
        filterPanel.add(manufacturerLabel);

        JTextField manufacturerProvCodeField = new JTextField();
        manufacturerProvCodeField.setFont(fieldFont);
        filterPanel.add(manufacturerProvCodeField);

        ////////CATEGORY///////////////////////////////////
        JLabel categoryLabel = new JLabel("CategoryTypeEn:");
        categoryLabel.setFont(labelFont);
        filterPanel.add(categoryLabel);

        JTextField categoryTypeEnField = new JTextField();
        categoryTypeEnField.setFont(fieldFont);
        filterPanel.add(categoryTypeEnField);

        ////////MILKTYPE///////////////////////////////////
        JLabel milkTypeLabel = new JLabel("MilkTypeEn:");
        milkTypeLabel.setFont(labelFont);
        filterPanel.add(milkTypeLabel);

        JTextField milkTypeEnField = new JTextField();
        milkTypeEnField.setFont(fieldFont);
        filterPanel.add(milkTypeEnField);
        ////PROCESING OPT///////////////////////////////
        JLabel processingLabel = new JLabel("Processing Option:");
        processingLabel.setFont(labelFont);
        filterPanel.add(processingLabel);

        JComboBox<String> processingOption = new JComboBox<>(new String[]{
            "Average value of moisture percent",
            "Percent of organic cheese"
        });
        processingOption.setFont(fieldFont);
        filterPanel.add(processingOption);

        JPanel buttonPanel = new JPanel();
        JButton searchButton = new JButton("Search");
        searchButton.setFont(labelFont);
        searchButton.setForeground(Color.WHITE);
        searchButton.setBackground(Color.BLUE);
        buttonPanel.add(searchButton);

        add(filterPanel, BorderLayout.NORTH);
        add(buttonPanel, BorderLayout.SOUTH);

        JTextArea resultArea = new JTextArea();
        resultArea.setFont(fieldFont);
        resultArea.setEditable(false);
        resultArea.setForeground(Color.DARK_GRAY);
        resultArea.setBackground(Color.WHITE);

        JScrollPane resultScrollPane = new JScrollPane(resultArea);
        resultScrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);

        add(resultScrollPane, BorderLayout.CENTER);

        setVisible(true);

        searchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String manufacturerProvCode = manufacturerProvCodeField.getText();
                String categoryTypeEn = categoryTypeEnField.getText();
                String milkTypeEn = milkTypeEnField.getText();
                int option = processingOption.getSelectedIndex();

                startHadoop(manufacturerProvCode, categoryTypeEn, milkTypeEn, option);

                resultArea.setText(results);
            }
        });
    }

        
    protected void startHadoop(String manufacturerProvCode, String categoryTypeEn, String milkTypeEn, int option) {
    		Configuration conf = new Configuration();
            
            Path inputPath = new Path("hdfs://127.0.0.1:9000/18.csv");
            Path outputPath = new Path("hdfs://127.0.0.1:9000/result");
            
            JobConf job = new JobConf(conf, App.class);
                        
            job.set("manufacturerProvCode", manufacturerProvCode);
            job.set("categoryTypeEn", categoryTypeEn);
            job.set("milkTypeEn", milkTypeEn);
            
            if (option == 0) {
                job.setMapperClass(MapperOptionOne.class);
                job.setReducerClass(ReducerOptionOne.class);
                job.setOutputKeyClass(Text.class);
                job.setMapOutputValueClass(DoubleWritable.class);
            } else {
                job.setMapperClass(MapperOptionTwo.class);
                job.setReducerClass(ReducerOptionTwo.class);
                job.setOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
            }
            
            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            
            try { 
  			  FileSystem fs = FileSystem.get(
  			  URI.create("hdfs://127.0.0.1:9000"),conf); 
  			  if(fs.exists(outputPath)) {
  				  fs.delete(outputPath, true); 
  			  } RunningJob task = JobClient.runJob(job);
  			  
  			  if(task.isSuccessful()) { 
  				  BufferedReader br = null;
  				  br = new BufferedReader(
  						  new InputStreamReader(fs.open(new Path("hdfs://127.0.0.1:9000/result/part-00000")))); 
  				  String line;
  				  StringBuilder sb = new StringBuilder();
  				  while ((line = br.readLine()) != null) { 
  					 sb.append(line);
  					 sb.append("\n");
  				  }
  				  results = sb.toString();
  			  }
  		  
  		  } catch (IOException e) { 
  			  System.out.println(e.toString()); 
  		  }
    	}
        
    }

