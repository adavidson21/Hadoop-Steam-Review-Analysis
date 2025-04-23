package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The main entry point for the application.
 * Requires 4 command-line arguments: input path, output path, positive word list path, negative word list path.
 */
public class Main {
    /**
     * Main class of the application.
     * Accepts the input and output parameters, sets up the different classes,
     * and starts the job.
     */
    public static void main(String[] args) throws Exception {
        // Validate parameter count
        if (args.length < 4) {
            System.err.println("Usage: Main <input_path> <output_path> <positive_words_path> <negative_words_path>");
            System.exit(1);
        }

        // Configure file paths
        Configuration conf = new Configuration();
        conf.set("positiveWordsPath", args[2]);
        conf.set("negativeWordsPath", args[3]);

        // Configure MapReduce job
        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(Main.class);

        // Set the mapper and reducer class
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        // Mapper outputs <Text, Text> pairs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer outputs <Text, Text> pairs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input/output formats and paths
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // Launch job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
