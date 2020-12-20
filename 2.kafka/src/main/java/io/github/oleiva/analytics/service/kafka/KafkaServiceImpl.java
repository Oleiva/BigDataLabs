package io.github.oleiva.analytics.service.kafka;


import io.github.oleiva.analytics.model.EventData;
import io.github.oleiva.analytics.service.analytics.Analytics;
import io.github.oleiva.analytics.service.configuration.ConsumerCreator;
import io.github.oleiva.analytics.service.output.OutputService;
import io.github.oleiva.analytics.service.parser.JsonParser;
import io.github.oleiva.analytics.service.parser.JsonParserImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.oleiva.analytics.service.configuration.IKafkaConstants.*;


@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaServiceImpl  implements KafkaService{
    private final JsonParser jsonParser;
    private final Analytics analytics;
    private final OutputService outputService;

    private List<EventData> topRecords = new ArrayList<>();

    public void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            try {

                if (consumerRecords.count() == 0) {
                    noMessageToFetch++;
                    if (noMessageToFetch > MAX_NO_MESSAGE_FOUND_COUNT)
                        break;
                    else
                        continue;
                }

                processBatch(consumerRecords);
                consumer.commitAsync();
            } catch (Exception e) {
                log.error(""+e);
            }

        }
        consumer.close();
    }


    private void processBatch(ConsumerRecords<Long, String> batchRecords) {
        log.info("BatchRecords size : "+batchRecords.count());
        List<EventData> records = parseRecords(batchRecords);
        List<EventData> top10Records = analytics.getTop10Records(records);

        outputService.printEventData(top10Records);
    }


    private List<EventData> parseRecords(ConsumerRecords<Long, String> batchRecords) {
        List<EventData> bitCoinMap = new ArrayList<>();

        batchRecords.forEach(record -> {
            EventData event = jsonParser.getRecord(record.value());
            bitCoinMap.add(event);
        });
        return bitCoinMap;

    }


}









