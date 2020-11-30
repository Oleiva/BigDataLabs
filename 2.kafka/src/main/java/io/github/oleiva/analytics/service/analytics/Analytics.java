package io.github.oleiva.analytics.service.analytics;

import io.github.oleiva.analytics.model.EventData;

import java.util.List;

public interface Analytics {
    List<EventData> getTop10Records(List<EventData> batchRecords);
}
