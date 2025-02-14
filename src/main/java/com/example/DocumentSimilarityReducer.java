package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DocumentSimilarityReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private HashSet<String> uniqueWords = new HashSet<>(); 
    private HashSet<String> commonWords = new HashSet<>(); 

    private OutputCollector outputCollector;
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        this.outputCollector=output;        
        HashSet<String> documentSet = new HashSet<>(); 

        
        while (values.hasNext()) {
            documentSet.add(values.next().toString());
        }
       
        uniqueWords.add(key.toString());
                if (documentSet.size() == 2) {
            commonWords.add(key.toString());
        }
    }

    @Override
    public void close() throws IOException {
        // Compute Jaccard Similarity = |A ∩ B| / |A ∪ B|
        double jaccardSimilarity = uniqueWords.isEmpty() ? 0.0 : (double) commonWords.size() / uniqueWords.size();

        // Output the Jaccard Similarity score
        outputCollector.collect(new Text("Jaccard Similarity:"), new Text(String.format("%.2f", jaccardSimilarity)));
    }
}