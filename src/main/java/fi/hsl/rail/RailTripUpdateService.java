package fi.hsl.rail;

import com.google.transit.realtime.*;
import lombok.extern.slf4j.*;
import org.apache.pulsar.client.api.*;

import java.util.*;

import static fi.hsl.rail.RailSpecific.*;

/**
 * Sends parsed railway alerts to a Pulsar topic
 */
@Slf4j
public
class RailTripUpdateService {
    private Producer<byte[]> producer;

    public RailTripUpdateService(Producer<byte[]> producer) {
        this.producer = producer;
    }

    int sendRailTripUpdates(com.google.transit.realtime.GtfsRealtime.FeedMessage feedMessage) {
        int sentTripUpdates = 0;

        List<GtfsRealtime.FeedEntity> feedEntities = filterRailTripUpdates(feedMessage);
        log.info("Found {} fi.hsl.rail trip updates", feedEntities.size());
        for (GtfsRealtime.FeedEntity feedEntity : feedEntities) {
            com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate = feedEntity.getTripUpdate();
            //Remove 'delay' field from trip update as stop time updates should be used to provide timing information
            tripUpdate = fixInvalidTripUpdateDelayUsage(tripUpdate);
            //Remove 'delay' field from stop time updates as raildigitraffic2gtfsrt API only provides inaccurate values
            tripUpdate = removeDelayFieldFromStopTimeUpdates(tripUpdate);
            //Remove 'trip_id' field from trip descriptor as we don't know if trip id provided by raildigitraffic2gtfsrt API is the same as in static GTFS feed used by Google and others
            tripUpdate = removeTripIdField(tripUpdate);

            sendTripUpdate(tripUpdate);
            sentTripUpdates++;
        }

        return sentTripUpdates;
    }

    private void sendTripUpdate(com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate) {
        long now = System.currentTimeMillis();

        String entityId = generateEntityId(tripUpdate);
        //String tripId = tripUpdate.getTrip().getTripId();
        com.google.transit.realtime.GtfsRealtime.FeedMessage feedMessage = fi.hsl.common.gtfsrt.FeedMessageFactory.createDifferentialFeedMessage(entityId, tripUpdate, now);

        producer.newMessage()
                .key(entityId)
                .value(feedMessage.toByteArray())
                .eventTime(now)
                .property(fi.hsl.common.transitdata.TransitdataProperties.KEY_PROTOBUF_SCHEMA, fi.hsl.common.transitdata.TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                .sendAsync()
                .whenComplete((messageId, throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof PulsarClientException) {
                            log.error("Failed to send message to Pulsar", throwable);
                        } else {
                            log.error("Unexpected error", throwable);
                        }
                    }

                    if (messageId != null) {
                        log.debug("Sending TripUpdate for entity {} with {} StopTimeUpdates and status {}",
                                entityId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship());
                    }
                });
    }

    static String generateEntityId(com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate) {
        return "rail_" + String.join("-", tripUpdate.getTrip().getRouteId(), tripUpdate.getTrip().getStartDate(), tripUpdate.getTrip().getStartTime(), String.valueOf(tripUpdate.getTrip().getDirectionId()));
    }
}
