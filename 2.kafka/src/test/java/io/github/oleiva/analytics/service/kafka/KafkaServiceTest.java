package io.github.oleiva.analytics.service.kafka;

import io.github.oleiva.analytics.model.EventData;
import io.github.oleiva.analytics.service.Mocks;
import io.github.oleiva.analytics.service.analytics.AnalyticsImpl;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
{
   "data":{
      "id":1297851689496576,
      "id_str":"1297851689496576",
      "order_type":1,
      "datetime":"1605693298",
      "microtimestamp":"1605693298239000",
      "amount":0.014,
      "amount_str":"0.01400000",
      "price":18058.68,
      "price_str":"18058.68"
   },
   "channel":"live_orders_btcusd",
   "event":"order_deleted"
}
 */
@RunWith(MockitoJUnitRunner.class)
class KafkaServiceTest extends Mocks {
    @InjectMocks
    private AnalyticsImpl analytics = new AnalyticsImpl();

    static List<EventData> batchRecords = new ArrayList<>();

    @BeforeAll
    static void setup() {
        System.out.println("@BeforeAll - executes once before all test methods in this class");

        List<EventData> data = getMock();

        batchRecords.addAll(data);
        System.out.println(batchRecords.size());
        MockitoAnnotations.initMocks(KafkaServiceTest.class);
    }


    @Test
    @Ignore
    void TestInit() {
        assertNotNull(batchRecords);
        assertEquals(100,batchRecords.size());
        batchRecords.forEach(x->{
            System.out.println(x.toString());
        });
    }



    @Test
    void getTop10Records() {
        batchRecords.forEach(x->{
            System.out.println(x.toString());
        });
        System.out.println("TOP: ");

        List<EventData> top_1  =analytics.getTop10Records(batchRecords);

        System.out.println(top_1.size());
        top_1.forEach(xx-> System.out.println(xx.toString()));

        assertTrue(top_1.get(0).getPrice()>=top_1.get(1).getPrice());
        assertTrue(top_1.get(2).getPrice()>=top_1.get(3).getPrice());
        assertEquals(10, top_1.size());

        List<EventData> top_2  =analytics.getTop10Records(getMock());

        System.out.println("TOP_2");
        assertTrue(top_2.get(0).getPrice()>=top_2.get(1).getPrice());
        assertTrue(top_2.get(2).getPrice()>=top_2.get(3).getPrice());
        assertEquals(10, top_2.size());

        top_2.forEach(xx-> System.out.println(xx.toString()));

        assertTrue(top_2.get(0).getPrice()>=top_1.get(0).getPrice());
        assertTrue(top_2.get(1).getPrice()>=top_1.get(1).getPrice());
        assertTrue(top_2.get(3).getPrice()>=top_1.get(3).getPrice());

    }



    @Test
    @Ignore
    void getTop10Recordsock() {

        List<EventData> mock = getDummyEventMock();
        mock.forEach(x->{
            System.out.println(x.toString());
        });
        System.out.println("TOP: 1");
        List<EventData> top_1  =analytics.getTop10Records(mock);
        top_1.forEach(e->{
            System.out.println(e.toString());
        });

        assertTrue(top_1.size()==1);
        EventData e = mock.get(0);
        e.setPrice(0.777);
        e.setId(343434L);
        List<EventData> eventDataList = new ArrayList<EventData>();
        eventDataList.add(e);
        List<EventData> top_2  =analytics.getTop10Records(eventDataList);
        System.out.println("TOP 2");
        top_2.forEach(e2->{
            System.out.println(e2.toString());
        });
    }



    private static List<EventData> getMock() {
        List<EventData> batchRecords = new ArrayList<>();
        long timeStamp = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            EventData data = new EventData();
            data.setId(timeStamp + i);
            data.setAmount(getAmount(0, 1, 1000));
            data.setPrice(getAmount(0, 1, 1000));
            data.setMicroTimeStamp(System.currentTimeMillis() + "");
            data.setEvent("order_deleted");
            data.setOrder_type(1);

            batchRecords.add(data);
        }
        return batchRecords;
    }


}