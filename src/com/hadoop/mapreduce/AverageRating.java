package com.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageRating {

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] rating = line.split("\t");

            if (rating.length >= 4) {
                int movieId = Integer.parseInt(rating[1]);
                double rate = Double.parseDouble(rating[2]);


                IntWritable mapKey = new IntWritable(movieId);
                DoubleWritable mapValue = new DoubleWritable(rate);
                context.write(mapKey, mapValue);
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double averageRating = 0;
            int numValues = 0;

            for (DoubleWritable v: values) {
                averageRating += v.get();
                numValues++;
            }
            averageRating /= numValues;

            DoubleWritable average = new DoubleWritable(averageRating);

            context.write(key, average);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Syntax: MovieRatings <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(AverageRating.class);
        job.setJobName("Average Rating");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

