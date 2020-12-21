package io.github.oleiva;

import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

public class BatchData extends AccumulatorV2<Tuple2<Integer, Long>, List<Long>>  {
    private List<Long> collector;

    BatchData() {
        collector = new ArrayList<>(Collections.nCopies(12, 0L));
    }

    private BatchData(List<Long> data) {
        collector = data;
    }

    @Override
    public boolean isZero() {
        for (int i = 0; i < 12; i++)
            if (collector.get(i) != 0){
                return false;
            }
        return true;
    }

    @Override
    public BatchData copy() {
        return new BatchData(new ArrayList<>(collector));
    }

    @Override
    public void reset() {
        for (int i = 0; i < 12; i++)
            collector.set(i, 0L);
    }

    @Override
    public void add(Tuple2<Integer, Long> v) {
        collector.set(v._1 - 1, collector.get(v._1 - 1) + v._2);
    }

    @Override
    public void merge(AccumulatorV2<Tuple2<Integer, Long>, List<Long>> other) {
        for (int i = 0; i < 12; i++)
            collector.set(i, collector.get(i) + other.value().get(i));
    }

    @Override
    public List<Long> value() {
        return collector;
    }
}