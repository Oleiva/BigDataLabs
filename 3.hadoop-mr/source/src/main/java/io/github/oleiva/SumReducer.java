package io.github.oleiva;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;


//public class SumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
public class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    // GLC| if you set the only reducer for the job
    // GLC| you can implement TopN algorithm right in here and write on Reducer.cleanup
    // GLC| so there is no need for extra MR jobs

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        // GLC: It should be double, shouldn't it?
        long departureDelay = 0;
        // GLC: It's better to use long here
        int count = 0;
        for (DoubleWritable val : values) {
            departureDelay += val.get();
            count++;
        }
        // GLC: departureDelay / count': integer division in floating-point context
        // GLC: Inspection info: Reports integer division where the result is either directly or indirectly
        // GLC: used as a floating point number. Such division is often an error, and may result in unexpected
        // GLC: results due to the truncation that happens in integer division.
        result.set(departureDelay / count);
        context.write(key, result);
    }

}
