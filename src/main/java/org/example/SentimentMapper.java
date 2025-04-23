package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The MapReduce Mapper class for sentiment analysis.
 * Reads in a .tsv of app names and reviews,
 * and emits pairs of app names to sentiment (e.g., "Undertale", "positive").
 */
public class SentimentMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Set<String> positiveWords = new HashSet<>();
    private final Set<String> negativeWords = new HashSet<>();
    private boolean headerSkipped = false;

    /**
     * Sets up the mapper.
     * Loads positive and negative word lists into memory which are used to match sentiment words in each review.
     * @param context The Hadoop context.
     * @throws IOException If I/O operations (i.e. reading file) fails.
     */
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        String positiveWordsPath = conf.get("positiveWordsPath");
        String negativeWordsPath = conf.get("negativeWordsPath");

        Path posPath = new Path(positiveWordsPath);
        Path negPath = new Path(negativeWordsPath);

        // Read positive words
        try (BufferedReader posReader = new BufferedReader(new InputStreamReader(fs.open(posPath)))) {
            String line;
            while ((line = posReader.readLine()) != null) {
                positiveWords.add(line.trim().toLowerCase());
            }
        }

        // Read negative words
        try (BufferedReader negReader = new BufferedReader(new InputStreamReader(fs.open(negPath)))) {
            String line;
            while ((line = negReader.readLine()) != null) {
                negativeWords.add(line.trim().toLowerCase());
            }
        }

    }

    /**
     * Maps each app name to its sentiment.
     * @param key Line offset (unused).
     * @param value A single line of input in TSV format (app_name \t review).
     * @param context The Hadoop context.
     * @throws IOException If I/O operations (i.e. reading file) fails.
     * @throws InterruptedException If failure occurs from interruption.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t"); // split on tab

        // Skip header
        if (!headerSkipped) {
            headerSkipped = true;
            return;
        }

        if (tokens.length < 2) {
            return;
        }

        String appName = tokens[0].trim();
        String reviewText = tokens[1].toLowerCase().replaceAll("[^a-zA-Z ]", " ");

        if (appName.isEmpty() || reviewText.isEmpty()) {
            return;
        }

        String[] words = reviewText.split("\\s+");

        int posCount = 0, negCount = 0;

        for (String word : words) {
            if (positiveWords.contains(word))
                posCount++;
            if (negativeWords.contains(word))
                negCount++;
        }

        String sentiment;
        if (posCount > negCount) {
            sentiment = "positive";
        } else if (negCount > posCount) {
            sentiment = "negative";
        } else {
            sentiment = "neutral";
        }

        context.write(new Text(appName), new Text(sentiment));
    }
}
