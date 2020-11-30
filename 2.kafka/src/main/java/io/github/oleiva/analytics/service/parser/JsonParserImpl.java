package io.github.oleiva.analytics.service.parser;
import io.github.oleiva.analytics.model.EventData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.*;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class JsonParserImpl implements JsonParser {

    public EventData getRecord(String jsonString) {

        JSONObject dataEvent = new JSONObject(jsonString);
        JSONObject data = dataEvent.getJSONObject("data");

        Long id = data.getLong("id");
        int orderType = data.getInt("order_type");
        String microTimeStamp = data.getString("microtimestamp");
        double amount = data.getDouble("amount");
        double price = data.getDouble("price");
        String event = dataEvent.getString("event");

        EventData eventData = new EventData();
        eventData.setId(id);
        eventData.setOrder_type(orderType);
        eventData.setMicroTimeStamp(microTimeStamp);
        eventData.setAmount(amount);
        eventData.setPrice(price);
        eventData.setEvent(event);

        return eventData;

    }
}

