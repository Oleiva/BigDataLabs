//package com.globallogic.bdpc.mapreduce.wordcount;
//
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//
//import java.util.Set;
//
//import static junit.framework.TestCase.assertTrue;
//import static org.junit.Assert.assertEquals;
//
//
////@RunWith(JUnit4.class)
//class TokenizerMapperTest {
////    private static TokenizerMapper mapper;
////    private static String LINE_SAMPL = "2015,1,1,4,UA,1197,N78448,SFO,IAH,0048,0042,-6,11,0053,218,217,199,1635,0612,7,0626,0619,-7,0,0,,,,,,";
//
//    @BeforeAll
//    static void setup() {
//        mapper = new TokenizerMapper();
//    }
//
//
//    @Test
//    void parseSkipFile() {
//        Set<String> setOfAirlines = mapper.parseSkipFile("src/test/resources/airlines.csv");
//        assertEquals(15, setOfAirlines.size());
//        assertTrue(setOfAirlines.contains("DL,Delta Air Lines Inc."));
//    }
//
//    @Test
//    void map() {
//
//    }
//
//    @Test
//    void mappingToLine() {
//        AirportWrapper airportWrapper = mapper.mappingToLine(LINE_SAMPL);
//        System.out.println(airportWrapper.toString());
//
//        assertEquals("SFO", airportWrapper.getIataCode().toString());
//        assertEquals(-6, airportWrapper.getAverageTuple().getDepartureDelay());
//        assertEquals(1, airportWrapper.getAverageTuple().getCount());
//    }
//}