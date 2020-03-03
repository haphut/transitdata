package fi.hsl.tripupdate.cancellations;

import com.google.protobuf.*;
import com.google.transit.realtime.*;
import fi.hsl.common.transitdata.proto.*;
import org.junit.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.junit.Assert.*;

public class HslCancellationAlertPollerTest {
    @Test
    public void testFeedMessages() throws Exception {
        testFeedMessage("three-entities.pb", 3);
        testFeedMessage("two-entities.pb", 2);
    }


    private void testFeedMessage(String filename, int expectedEntities) throws Exception {
        URL url = getTestResource(filename);
        GtfsRealtime.FeedMessage feed = HSlCancellationAlertPoller.readFeedMessage(url);
        assertNotNull(feed);

        assertEquals(expectedEntities, feed.getEntityCount());

        List<GtfsRealtime.TripUpdate> tripUpdates = HSlCancellationAlertPoller.getTripUpdates(feed);
        assertEquals(1, tripUpdates.size());

        for (GtfsRealtime.TripUpdate update : tripUpdates) {
            validateInternalMessage(update);
        }
    }

    private URL getTestResource(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return classLoader.getResource(name);
    }

    private void validateInternalMessage(GtfsRealtime.TripUpdate update) {
        final GtfsRealtime.TripDescriptor trip = update.getTrip();
        final int joreDirection = trip.getDirectionId() + 1;
        final InternalMessages.TripCancellation cancellation = HSlCancellationAlertPoller.createPulsarPayload(trip, joreDirection, trip.getTripId(), InternalMessages.TripCancellation.Status.CANCELED, Optional.empty());

        assertEquals(trip.getTripId(), cancellation.getTripId());
        assertEquals(joreDirection, cancellation.getDirectionId());
        assertEquals(trip.getRouteId(), cancellation.getRouteId());

        assertNotNull(cancellation.getStartTime());
        assertEquals(trip.getStartTime(), cancellation.getStartTime());

        assertNotNull(cancellation.getStartDate());
        assertEquals(trip.getStartDate(), cancellation.getStartDate());
        assertEquals(1, cancellation.getSchemaVersion());

        assertEquals(InternalMessages.TripCancellation.Status.CANCELED, cancellation.getStatus());
    }

    @Test(expected = IOException.class)
    public void testUrlError() throws IOException {
        HSlCancellationAlertPoller.readFeedMessage("invalid-url");
    }

    @Test(expected = IOException.class)
    public void testHttpError() throws IOException {
        HSlCancellationAlertPoller.readFeedMessage("http://does.not.exist");
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void testProtobufError() throws IOException {
        URL textFile = getTestResource("test.txt");
        HSlCancellationAlertPoller.readFeedMessage(textFile);
    }
}
