package io.github.oleiva.analytics.service;

import io.github.oleiva.analytics.model.EventData;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Mocks {
    public List<EventData> getDummyEventMock(){
        List<EventData> batchRecords = new ArrayList<>();
        long timeStamp = System.currentTimeMillis();
        EventData data= dummyEvent();
        for (int i = 0; i < 100; i++) {
            batchRecords.add(data);
        }
        return batchRecords;
    }

    public EventData dummyEvent(){
        EventData data = new EventData();
        data.setId(System.currentTimeMillis());
        data.setAmount(getAmount(0, 1, 1000));
        data.setPrice(getAmount(0, 1, 1000));
        data.setMicroTimeStamp(System.currentTimeMillis() + "");
        data.setEvent("order_deleted");
        data.setOrder_type(1);
        return data;
    }
    public static double getAmount(int rangeMin, int rangeMax, int range) {
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
        return (double) Math.round(randomValue * range) / range;
    }
}
