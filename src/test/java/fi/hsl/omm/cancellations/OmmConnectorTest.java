package fi.hsl.omm.cancellations;

import org.junit.jupiter.api.*;

import java.time.*;

import static org.junit.jupiter.api.Assertions.*;

public class OmmConnectorTest {
    @Test
    public void testTimestampAsString() {
        assertEquals(OmmConnector.localDatetimeAsString(Instant.ofEpochSecond(1541415600), "Europe/Helsinki"), "2018-11-05 13:00:00");
        assertEquals(OmmConnector.localDatetimeAsString(Instant.ofEpochSecond(1541422800), "UTC"), "2018-11-05 13:00:00");

        assertEquals(OmmConnector.localDatetimeAsString(Instant.ofEpochSecond(1514764800), "UTC"), "2018-01-01 00:00:00");
        assertEquals(OmmConnector.localDatetimeAsString(Instant.ofEpochSecond(1514843999), "Europe/Helsinki"), "2018-01-01 23:59:59");

    }
}
