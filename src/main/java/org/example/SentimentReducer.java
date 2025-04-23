package org.example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reducer class for aggregating sentiment counts per app.
 * For each app name, it receives a list of sentiment labels and counts how many were positive, negative, and neutral.
 */
public class SentimentReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text appName, Iterable<Text> sentiments, Context context)
            throws IOException, InterruptedException {

        int positive = 0;
        int negative = 0;
        int neutral = 0;

        // Count occurrences of each sentiment type
        for (Text sentiment : sentiments) {
            switch (sentiment.toString()) {
                case "positive":
                    positive++;
                    break;
                case "negative":
                    negative++;
                    break;
                case "neutral":
                    neutral++;
                    break;
            }
        }

        String result = String.format("Positive: %d, Negative: %d, Neutral: %d", positive, negative, neutral);  // Format output
        context.write(appName, new Text(result));
    }
}