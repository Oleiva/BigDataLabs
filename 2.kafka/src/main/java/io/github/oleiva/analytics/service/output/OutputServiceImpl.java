package io.github.oleiva.analytics.service.output;

import io.github.oleiva.analytics.model.EventData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class OutputServiceImpl implements OutputService {
    private static final String PATTERN_OUTPUT = "|%20s|%20s|%10s|";
    private static final String PATTERN_OUTPUT2 = "|%25s|%8s|";
    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\033[0;32m";
    private static final String BLUE = "\u001B[34m";


    public void printEventData(List<EventData> top10Records){
        System.out.println("TOP 10 BITCOIN TRANSACTIONS");
        System.out.println("____________________________________________________________________________________________________________________");
        System.out.println("# | id | microTimeStamp           | type | price | amount | event");
        System.out.println("____________________________________________________________________________________________________________________");
        int i =0;
        for (EventData rec : top10Records) {
            System.out.println(""+i+" | " + rec.getId() + " | " + rec.getMicroTimeStamp() + " | " + rec.getOrder_type() + " |  " + rec.getPrice() + " | " + rec.getAmount()+" | "+rec.getEvent());
            i++;
        }
        System.out.println("____________________________________________________________________________________________________________________");
    }
}
