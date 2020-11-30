package io.github.oleiva.analytics.service.output;

import io.github.oleiva.analytics.model.EventData;

import java.util.List;

public interface OutputService {
    void printEventData(List<EventData> top10Records);
}
