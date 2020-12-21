package io.github.oleiva;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

    public class TopAirports {
    private enum Fields {YEAR, IATA_CODE}

    private JavaPairRDD<Integer, Tuple3<String, String, Integer>> maxInMonth;


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] sparkArgs = optionParser.getRemainingArgs();
        if (sparkArgs.length != 3) {
            System.err.println("Wrong format: -flight -airports -outFolder");
            System.exit(2);
        }

        System.out.println("arg[0] "+sparkArgs[0]);
        System.out.println("arg[1] "+sparkArgs[1]);
        System.out.println("arg[2] "+sparkArgs[2]);

        new TopAirports().calculate(sparkArgs[0], sparkArgs[1]);
        new TopAirports().output(sparkArgs[2]);
    }

    public JavaRDD<String[]> getFlightsCsvToRDD(JavaSparkContext sc, String filename) {
        return sc.textFile(filename)
                .map(line -> line.split(","))
                .filter(line -> !line[0].equals(Fields.YEAR.name()));
    }

    public JavaPairRDD<String, String> getAirportsCsvToRDD(JavaSparkContext sc, String filename) {
        return sc.textFile(filename).mapToPair(line -> {
            String[] v = line.split(",");
            return new Tuple2<>(v[0], v[1]);
        }).filter(line -> !line._1().equals(Fields.IATA_CODE.name()));
    }

    public Map<String, BatchData> createAccumulators(JavaSparkContext sc, Map<String, String> airportsData) {
        Map<String, BatchData> airportsAccums = new TreeMap<>();
        for (String key : airportsData.keySet()) {
            BatchData batchData = new BatchData();
            sc.sc().register(batchData, key);
            airportsAccums.put(key, batchData);
        }

        return airportsAccums;
    }


    private JavaPairRDD<Integer, Tuple3<String, String, Integer>> getTopAirportsByMonth(
            JavaRDD<String[]> input,
            Broadcast<Map<String, String>> airportsData,
            Map<String, BatchData> batchData) {
        return input.mapToPair(elems -> new Tuple2<>(elems[1] + "_" + elems[8], 1))
                .reduceByKey(Integer::sum)
                .mapToPair(row -> {
                    String[] keys = row._1.split("_");
                    int topMonth = Integer.parseInt(keys[0]);
                    if (batchData != null && batchData.containsKey(keys[1]))
                        batchData.get(keys[1]).add(new Tuple2<>(topMonth, row._2.longValue()));
                    return new Tuple2<>(topMonth, new Tuple3<>(keys[1], airportsData.value().getOrDefault(keys[1], "Unknown"), row._2));
                })
                .reduceByKey((r1, r2) -> r1._3() > r2._3() ? r1 : r2)
                .sortByKey();
    }


    private void calculate(String flights, String airports) {
        System.out.println("calculate : ");
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("TOPAirportByMonth").setMaster("yarn"));

        JavaRDD<String[]> parsedCSV = getFlightsCsvToRDD(sc, flights);
        Broadcast<Map<String, String>> airportsData = sc.broadcast(getAirportsCsvToRDD(sc, airports).collectAsMap());

        Map<String, BatchData> airportsAccums = createAccumulators(sc, airportsData.value());
        maxInMonth = getTopAirportsByMonth(parsedCSV, airportsData, airportsAccums);

        airportsAccums.keySet().stream().map(airport -> airport + ": " + airportsAccums.get(airport).value()).forEach(System.out::println);
        sc.close();
    }

    private void output(String output) {
        System.out.println("output: ");
        if (maxInMonth != null) {
            maxInMonth.map(vals -> vals._1.toString() + " " + vals._2.productIterator().mkString(" ")).saveAsTextFile(output);
        }
    }


}
