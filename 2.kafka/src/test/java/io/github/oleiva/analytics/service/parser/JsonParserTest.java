package io.github.oleiva.analytics.service.parser;

import io.github.oleiva.analytics.model.EventData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonParserTest {

    @Test
    void getRecord() {
        JsonParserImpl parserJSON = new JsonParserImpl();
        final String jsonString = "{\"data\":{\"id\":1297851689496576,\"id_str\":\"1297851689496576\",\"order_type\":1,\"datetime\":\"1605693298\",\"microtimestamp\":\"1605693298239000\",\"amount\":0.014,\"amount_str\":\"0.01400000\",\"price\":18058.68,\"price_str\":\"18058.68\"},\"channel\":\"live_orders_btcusd\",\"event\":\"order_deleted\"}";
        EventData eventData = parserJSON.getRecord(jsonString);
        eventData.getAmount();
        eventData.getId();
        assertEquals(0.014,eventData.getAmount());
    }

    //    public static void main(String[] args) {
//        JsonParserImpl parserJSON = new JsonParserImpl();
//        final String jsonString = "{\"data\":{\"id\":1297851689496576,\"id_str\":\"1297851689496576\",\"order_type\":1,\"datetime\":\"1605693298\",\"microtimestamp\":\"1605693298239000\",\"amount\":0.014,\"amount_str\":\"0.01400000\",\"price\":18058.68,\"price_str\":\"18058.68\"},\"channel\":\"live_orders_btcusd\",\"event\":\"order_deleted\"}";
//        parserJSON.getRecord(jsonString);
//    }
}