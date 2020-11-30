package io.github.oleiva.analytics.service.analytics;

import io.github.oleiva.analytics.model.EventData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

@Slf4j
@Component
public class AnalyticsImpl implements Analytics {
    private List<EventData> topRecords = new ArrayList<>();

    public List<EventData> getTop10Records(List<EventData> batchRecords) {
        batchRecords.addAll(topRecords);

        List<EventData> eventDataList = batchRecords
                .stream()
                .distinct()
                .sorted(comparing(EventData::getPrice, comparing(Math::abs)).reversed())
                .limit(10)
                .collect(toList());
        topRecords = eventDataList;
        return topRecords;

    }
}
