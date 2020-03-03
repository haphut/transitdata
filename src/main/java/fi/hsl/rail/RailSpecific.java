package fi.hsl.rail;

import com.google.transit.realtime.*;
import lombok.extern.slf4j.*;

import java.util.*;
import java.util.stream.*;

@Slf4j
class RailSpecific {
    static List<GtfsRealtime.FeedEntity> filterRailTripUpdates(com.google.transit.realtime.GtfsRealtime.FeedMessage feedMessage) {
        return feedMessage.getEntityList()
                .stream()
                .filter(com.google.transit.realtime.GtfsRealtime.FeedEntity::hasTripUpdate)
                .collect(Collectors.toList());
    }

    static com.google.transit.realtime.GtfsRealtime.TripUpdate fixInvalidTripUpdateDelayUsage(com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate) {
        //If trip update has specified delay and stop time updates, timing information in stop time updates is ignored
        //-> remove delay from trip update
        if (tripUpdate.hasDelay() && tripUpdate.getStopTimeUpdateCount() != 0) {
            return tripUpdate.toBuilder().clearDelay().build();
        } else {
            return tripUpdate;
        }
    }

    static com.google.transit.realtime.GtfsRealtime.TripUpdate removeDelayFieldFromStopTimeUpdates(com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate) {
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream()
                .map(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate::toBuilder)
                .map(stuBuilder -> {
                    if (stuBuilder.hasArrival()) {
                        stuBuilder = stuBuilder.setArrival(removeDelayField(stuBuilder.getArrival()));
                    }
                    if (stuBuilder.hasDeparture()) {
                        stuBuilder = stuBuilder.setDeparture(removeDelayField(stuBuilder.getDeparture()));
                    }
                    return stuBuilder.build();
                })
                .collect(Collectors.toList());

        return tripUpdate.toBuilder().clearStopTimeUpdate().addAllStopTimeUpdate(stopTimeUpdates).build();
    }

    private static com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent removeDelayField(com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent stopTimeEvent) {
        if (stopTimeEvent.hasDelay() && stopTimeEvent.hasTime()) {
            return stopTimeEvent.toBuilder().clearDelay().build();
        } else {
            return stopTimeEvent;
        }
    }

    static com.google.transit.realtime.GtfsRealtime.TripUpdate removeTripIdField(com.google.transit.realtime.GtfsRealtime.TripUpdate tripUpdate) {
        if (tripUpdate.hasTrip() && tripUpdate.getTrip().hasTripId()) {
            return tripUpdate.toBuilder()
                    .setTrip(tripUpdate.getTrip().toBuilder().clearTripId())
                    .build();
        } else {
            return tripUpdate;
        }
    }
}
