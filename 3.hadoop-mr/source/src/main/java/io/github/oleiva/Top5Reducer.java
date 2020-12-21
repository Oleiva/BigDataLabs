package io.github.oleiva;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Top5Reducer extends Reducer<Text, LongWritable, LongWritable, Text> {

    private TreeMap<Long, String> tmap2;
    private int TOP_SIZE =5;


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tmap2 = new TreeMap<Long, String>();
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        String name = key.toString();
        long count = 0;

        for (LongWritable val : values) {
            count = val.get();
        }

        tmap2.put(count, name);

        if (tmap2.size() > TOP_SIZE) {
            tmap2.remove(tmap2.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : tmap2.entrySet()) {
            long count = entry.getKey();
            String name = entry.getValue();
            context.write(new LongWritable(count), new Text(name));
        }
    }
}
