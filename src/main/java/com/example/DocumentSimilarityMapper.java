package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class DocumentSimilarityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private Text wordKey = new Text();      
    private Text fileNameValue = new Text(); 

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        fileNameValue.set(fileName); 

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()) {
            // Extract next word and clean it (remove punctuation, convert to lowercase)
            String word = tokenizer.nextToken().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();

            // Ensure the word is not empty after cleaning
            if (!word.isEmpty()) {
                wordKey.set(word); // Set the word as the key
                output.collect(wordKey, fileNameValue); // Emit the (word, filename) pair
            }
        }
    }
}