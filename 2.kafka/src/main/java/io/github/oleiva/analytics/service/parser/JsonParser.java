package io.github.oleiva.analytics.service.parser;

import io.github.oleiva.analytics.model.EventData;

public interface JsonParser {
    EventData getRecord(String jsonString);
}
