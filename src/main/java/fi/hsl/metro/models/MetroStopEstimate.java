package fi.hsl.metro.models;

import fi.hsl.metro.*;
import lombok.*;

import java.util.*;

@Data
public class MetroStopEstimate {
    private long routerowId;
    private String station;
    private String platform;
    private String source;
    private MetroProgress rowProgress;
    private String arrivalTimePlanned;
    private String arrivalTimeForecast;
    private String arrivalTimeMeasured;
    private String departureTimePlanned;
    private String departureTimeForecast;
    private String departureTimeMeasured;

    public void setArrivalTimePlanned(String arrivalTimePlanned) {
        Optional<String> maybeArrivalTimePlanned = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(arrivalTimePlanned);
        this.arrivalTimePlanned = maybeArrivalTimePlanned.orElse(null);
    }

    public void setArrivalTimeForecast(String arrivalTimeForecast) {
        Optional<String> maybeArrivalTimeForecast = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(arrivalTimeForecast);
        this.arrivalTimeForecast = maybeArrivalTimeForecast.orElse(null);
    }

    public void setArrivalTimeMeasured(String arrivalTimeMeasured) {
        Optional<String> maybeArrivalTimeMeasured = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(arrivalTimeMeasured);
        this.arrivalTimeMeasured = maybeArrivalTimeMeasured.orElse(null);
    }

    public void setDepartureTimePlanned(String departureTimePlanned) {
        Optional<String> maybeDepartureTimePlanned = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(departureTimePlanned);
        this.departureTimePlanned = maybeDepartureTimePlanned.orElse(null);
    }

    public void setDepartureTimeForecast(String departureTimeForecast) {
        Optional<String> maybeDepartureTimeForecast = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(departureTimeForecast);
        this.departureTimeForecast = maybeDepartureTimeForecast.orElse(null);
    }

    public void setDepartureTimeMeasured(String departureTimeMeasured) {
        Optional<String> maybeDepartureTimeMeasured = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(departureTimeMeasured);
        this.departureTimeMeasured = maybeDepartureTimeMeasured.orElse(null);
    }
}
