package io.github.oleiva;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;


//public class SumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
public class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        long departureDelay = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            departureDelay += val.get();
            count++;
        }

        result.set(departureDelay / count);
        context.write(key, result);
    }

}
