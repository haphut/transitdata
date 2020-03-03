package fi.hsl.omm.alerts.models;

import fi.hsl.common.transitdata.proto.*;
import lombok.*;

import java.time.*;
import java.util.*;

import static java.util.Optional.*;

@Data
public class Bulletin {

    private long id;
    private Impact impact;
    private Category category;
    private LocalDateTime lastModified;
    private Optional<LocalDateTime> validFrom;
    private Optional<LocalDateTime> validTo;
    private boolean affectsAllRoutes;
    private boolean affectsAllStops;
    private List<Long> affectedLineGids;
    private List<Long> affectedStopGids;
    private Priority priority;
    private List<InternalMessages.Bulletin.Translation> titles;
    private List<InternalMessages.Bulletin.Translation> descriptions;
    private List<InternalMessages.Bulletin.Translation> urls;

    public Bulletin() {
    }

    public Bulletin(Bulletin other) {
        id = other.id;
        category = other.getCategory();
        impact = other.impact;
        lastModified = other.lastModified;
        validFrom = other.validFrom;
        validTo = other.validTo;
        affectsAllRoutes = other.affectsAllRoutes;
        affectsAllStops = other.affectsAllStops;
        if (other.affectedLineGids != null)
            affectedLineGids = new LinkedList<>(other.affectedLineGids);
        if (other.affectedStopGids != null)
            affectedStopGids = new LinkedList<>(other.affectedStopGids);
        priority = other.priority;
        if (other.titles != null)
            titles = new ArrayList<>(other.titles);
        if (other.descriptions != null)
            descriptions = new ArrayList<>(other.descriptions);
        if (other.urls != null)
            urls = new ArrayList<>(other.urls);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Bulletin) {
            return equals((Bulletin) other);
        }
        return false;
    }

    public boolean equals(Bulletin other) {
        if (other == this)
            return true;

        if (other == null)
            return false;

        boolean same = true;
        same &= this.id == other.id;
        same &= this.category == other.category;
        same &= this.impact == other.impact;
        same &= equalsWithNullCheck(this.lastModified, other.lastModified);
        same &= equalsWithNullCheck(this.validFrom, other.validFrom);
        same &= equalsWithNullCheck(this.validTo, other.validTo);
        same &= this.affectsAllRoutes == other.affectsAllRoutes;
        same &= this.affectsAllStops == other.affectsAllStops;
        same &= equalsWithNullCheck(this.affectedLineGids, other.affectedLineGids);
        same &= equalsWithNullCheck(this.affectedStopGids, other.affectedStopGids);
        same &= this.priority == other.priority;
        same &= equalsWithNullCheck(this.titles, other.titles);
        same &= equalsWithNullCheck(this.descriptions, other.descriptions);
        same &= equalsWithNullCheck(this.urls, other.urls);

        return same;
    }

    static boolean equalsWithNullCheck(Object o1, Object o2) {
        if (o1 == null && o2 == null)
            return true;
        if (o1 != null && o2 != null)
            return o1.equals(o2);
        return false;
    }

    public enum Category {

        OTHER_DRIVER_ERROR,
        ITS_SYSTEM_ERROR,
        TOO_MANY_PASSENGERS,
        MISPARKED_VEHICLE,
        STRIKE,
        TEST,
        VEHICLE_OFF_THE_ROAD,
        TRAFFIC_ACCIDENT,
        SWITCH_FAILURE,
        SEIZURE,
        WEATHER,
        STATE_VISIT,
        ROAD_MAINTENANCE,
        ROAD_CLOSED,
        TRACK_BLOCKED,
        WEATHER_CONDITIONS,
        ASSAULT,
        TRACK_MAINTENANCE,
        MEDICAL_INCIDENT,
        EARLIER_DISRUPTION,
        TECHNICAL_FAILURE,
        TRAFFIC_JAM,
        OTHER,
        NO_TRAFFIC_DISRUPTION,
        ACCIDENT,
        PUBLIC_EVENT,
        ROAD_TRENCH,
        VEHICLE_BREAKDOWN,
        POWER_FAILURE,
        STAFF_DEFICIT,
        DISTURBANCE,
        VEHICLE_DEFICIT;

        public static Category fromString(String str) {
            switch (str) {
                case "OTHER_DRIVER_ERROR":
                    return OTHER_DRIVER_ERROR;
                case "ITS_SYSTEM_ERROR":
                    return ITS_SYSTEM_ERROR;
                case "TOO_MANY_PASSENGERS":
                    return TOO_MANY_PASSENGERS;
                case "MISPARKED_VEHICLE":
                    return MISPARKED_VEHICLE;
                case "STRIKE":
                    return STRIKE;
                case "TEST":
                    return TEST;
                case "VEHICLE_OFF_THE_ROAD":
                    return VEHICLE_OFF_THE_ROAD;
                case "TRAFFIC_ACCIDENT":
                    return TRAFFIC_ACCIDENT;
                case "SWITCH_FAILURE":
                    return SWITCH_FAILURE;
                case "SEIZURE":
                    return SEIZURE;
                case "WEATHER":
                    return WEATHER;
                case "STATE_VISIT":
                    return STATE_VISIT;
                case "ROAD_MAINTENANCE":
                    return ROAD_MAINTENANCE;
                case "ROAD_CLOSED":
                    return ROAD_CLOSED;
                case "TRACK_BLOCKED":
                    return TRACK_BLOCKED;
                case "WEATHER_CONDITIONS":
                    return WEATHER_CONDITIONS;
                case "ASSAULT":
                    return ASSAULT;
                case "TRACK_MAINTENANCE":
                    return TRACK_MAINTENANCE;
                case "MEDICAL_INCIDENT":
                    return MEDICAL_INCIDENT;
                case "EARLIER_DISRUPTION":
                    return EARLIER_DISRUPTION;
                case "TECHNICAL_FAILURE":
                    return TECHNICAL_FAILURE;
                case "TRAFFIC_JAM":
                    return TRAFFIC_JAM;
                case "OTHER":
                    return OTHER;
                case "NO_TRAFFIC_DISRUPTION":
                    return NO_TRAFFIC_DISRUPTION;
                case "ACCIDENT":
                    return ACCIDENT;
                case "PUBLIC_EVENT":
                    return PUBLIC_EVENT;
                case "ROAD_TRENCH":
                    return ROAD_TRENCH;
                case "VEHICLE_BREAKDOWN":
                    return VEHICLE_BREAKDOWN;
                case "POWER_FAILURE":
                    return POWER_FAILURE;
                case "STAFF_DEFICIT":
                    return STAFF_DEFICIT;
                case "DISTURBANCE":
                    return DISTURBANCE;
                case "VEHICLE_DEFICIT":
                    return VEHICLE_DEFICIT;
                default:
                    throw new IllegalArgumentException("Could not parse category from String: " + str);
            }
        }

        public InternalMessages.Category toCategory() {
            return InternalMessages.Category.valueOf(this.toString());
        }
    }


    public enum Impact {
        CANCELLED,
        DELAYED,
        DEVIATING_SCHEDULE,
        DISRUPTION_ROUTE,
        IRREGULAR_DEPARTURES,
        POSSIBLE_DEVIATIONS,
        POSSIBLY_DELAYED,
        REDUCED_TRANSPORT,
        RETURNING_TO_NORMAL,
        VENDING_MACHINE_OUT_OF_ORDER,
        NULL,
        OTHER,
        NO_TRAFFIC_IMPACT,
        UNKNOWN;

        public static Impact fromString(String str) {
            if (str == null) {
                return NULL; //This can be null in the database schema.
            }
            switch (str) {
                case "CANCELLED":
                    return CANCELLED;
                case "DELAYED":
                    return DELAYED;
                case "DEVIATING_SCHEDULE":
                    return DEVIATING_SCHEDULE;
                case "DISRUPTION_ROUTE":
                    return DISRUPTION_ROUTE;
                case "IRREGULAR_DEPARTURES":
                    return IRREGULAR_DEPARTURES;
                case "POSSIBLE_DEVIATIONS":
                    return POSSIBLE_DEVIATIONS;
                case "POSSIBLY_DELAYED":
                    return POSSIBLY_DELAYED;
                case "REDUCED_TRANSPORT":
                    return REDUCED_TRANSPORT;
                case "RETURNING_TO_NORMAL":
                    return RETURNING_TO_NORMAL;
                case "VENDING_MACHINE_OUT_OF_ORDER":
                    return VENDING_MACHINE_OUT_OF_ORDER;
                case "OTHER":
                    return OTHER;
                case "NO_TRAFFIC_IMPACT":
                    return NO_TRAFFIC_IMPACT;
                case "UNKNOWN":
                    return UNKNOWN;
                default:
                    throw new IllegalArgumentException("Could not parse Impact from String: " + str);
            }
        }

        public InternalMessages.Bulletin.Impact toImpact() {
            return InternalMessages.Bulletin.Impact.valueOf(this.toString());
        }
    }

    public enum Language {
        //Let's define these already in BCP-47 format, so .toString() works
        fi, en, sv
    }

    public enum Priority {
        INFO,
        WARNING,
        SEVERE;

        public static Optional<Priority> fromInt(final Integer priority) {
            switch (priority) {
                case 1:
                    return of(INFO);
                case 2:
                    return of(WARNING);
                case 3:
                    return of(SEVERE);
                default:
                    return empty();
            }
        }

        public InternalMessages.Bulletin.Priority toPriority() {
            return InternalMessages.Bulletin.Priority.valueOf(this.toString());
        }
    }

}
