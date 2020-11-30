package io.github.oleiva.analytics.service.output;

import io.github.oleiva.analytics.service.Mocks;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OutputServiceTest extends Mocks {

    @Test
    void printEventData() {
        OutputService outputService = new OutputServiceImpl();

        outputService.printEventData(getDummyEventMock());
    }
}