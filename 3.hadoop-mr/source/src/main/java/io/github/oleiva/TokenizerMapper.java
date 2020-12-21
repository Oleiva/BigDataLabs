package io.github.oleiva;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

//https://timepasstechies.com/map-reduce-pattern-calculating-average-sample/
public class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {


    private final static DoubleWritable doubleWrite = new DoubleWritable(1L);


    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    private Text aerodrome = new Text();


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
        if (conf.getBoolean("wordcount.skip.patterns", false)) {
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName);
            }
        }
    }

    public Set<String> parseSkipFile(String fileName) {
        try {
            fis = new BufferedReader(new FileReader(fileName));
            String pattern = null;
            while ((pattern = fis.readLine()) != null) {
                patternsToSkip.add(pattern);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
        }
        return patternsToSkip;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

           String[] field = line.split(",");
           if (field[11].equals("DEPARTURE_DELAY")){
               return;
           }
           if (field[8].length()>0 &field[11].length()>0){


           String iataCodePerLine = field[8];
           long departureDelayPerLine = Long.parseLong(field[11]);


           aerodrome.set(iataCodePerLine);
           doubleWrite.set(departureDelayPerLine);
           context.write(aerodrome, doubleWrite);
           }

    }


}