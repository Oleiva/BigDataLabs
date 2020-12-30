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

        // GLC| What about key collisions? I'd use TreeSet
        tmap.put(Long.parseLong(value.toString()), key.toString());

        // GLC: Very nice idea to minimize maps output, though it can be also done with Combiner
        if (tmap.size() > TOP_SIZE) {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : tmap.entrySet()) {

            long count = entry.getKey();
            String name = entry.getValue();

            // GLC: It's better to reuse writables
            context.write(new Text(name), new LongWritable(count));
        }
    }
}
