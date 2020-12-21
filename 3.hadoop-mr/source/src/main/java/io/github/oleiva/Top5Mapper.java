package io.github.oleiva;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Top5Mapper extends Mapper<Object, Text, Text, LongWritable> {
    private int TOP_SIZE =5;

    private TreeMap<Long, String> tmap;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tmap = new TreeMap<Long, String>();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        tmap.put(Long.parseLong(value.toString()), key.toString());

        if (tmap.size() > TOP_SIZE) {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : tmap.entrySet()) {

            long count = entry.getKey();
            String name = entry.getValue();

            context.write(new Text(name), new LongWritable(count));
        }
    }
}
