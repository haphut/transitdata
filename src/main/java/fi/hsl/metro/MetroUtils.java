package fi.hsl.metro;

import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.transitdata.*;
import org.apache.pulsar.shade.com.google.common.collect.*;
import org.slf4j.*;

import java.time.*;
import java.time.format.*;
import java.util.Optional;
import java.util.*;

public class MetroUtils {
    private static final Logger log = LoggerFactory.getLogger(MetroUtils.class);

    private static final HashMap<String, String> shortNameByStopNumber = new HashMap<>();
    private static final ArrayListMultimap<String, String> stopNumbersByShortName = ArrayListMultimap.create();
    private static final List<String> shortNames = new ArrayList<>();
    private static final DateTimeFormatter dateTimeFormatter;
    private static final DateTimeFormatter metroAtsDateTimeFormatter;
    private static final DateTimeFormatter pubtransDateTimeFormatter;
    private static final DateTimeFormatter utcDateTimeFormatter;
    private static final ZoneId metroAtsZoneId;
    private static final ZoneId pubtransZoneId;
    private static final ZoneId utcZoneId;

    static {
        final Config stopsConfig = ConfigParser.createConfig("metro_stops.conf");
        stopsConfig.getObjectList("metroStops")
                .forEach(stopConfigObject -> {
                    final Config stopConfig = stopConfigObject.toConfig();
                    final String shortName = stopConfig.getString("shortName");
                    final List<String> stopNumbers = stopConfig.getStringList("stopNumbers");
                    shortNames.add(shortName);
                    stopNumbers.forEach(stopNumber -> {
                        shortNameByStopNumber.put(stopNumber, shortName);
                        stopNumbersByShortName.put(shortName, stopNumber);
                    });
                });
        final Config config = ConfigParser.createConfig();
        final String metroAtsTimeZone = config.getString("application.metroAtstimezone");
        final String pubtransTimeZone = config.getString("application.pubtransTimezone");
        metroAtsZoneId = ZoneId.of(metroAtsTimeZone);
        pubtransZoneId = ZoneId.of(pubtransTimeZone);
        utcZoneId = ZoneId.of("UTC");
        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        metroAtsDateTimeFormatter = dateTimeFormatter.withZone(metroAtsZoneId);
        pubtransDateTimeFormatter = dateTimeFormatter.withZone(pubtransZoneId);
        utcDateTimeFormatter = dateTimeFormatter.withZone(utcZoneId);
    }

    private MetroUtils() {
    }

    public static Optional<String> getShortName(final String stopNumber) {
        return Optional.ofNullable(shortNameByStopNumber.get(stopNumber));
    }

    public static Optional<String> getStopNumber(final String shortName, final String startStopShortName, final String endStopShortName) {
        final Optional<Integer> joreDirection = getJoreDirection(startStopShortName, endStopShortName);
        return joreDirection.isPresent() ? getStopNumber(shortName, joreDirection.get()) : Optional.empty();
    }

    public static Optional<Integer> getJoreDirection(final String startStop, final String endStop) {
        final int startStopIndex = shortNames.indexOf(startStop);
        final int endStopIndex = shortNames.indexOf(endStop);
        if (startStopIndex == -1 || endStopIndex == -1 || startStopIndex == endStopIndex) {
            return Optional.empty();
        }
        return Optional.of(startStopIndex < endStopIndex ? PubtransFactory.JORE_DIRECTION_ID_OUTBOUND : PubtransFactory.JORE_DIRECTION_ID_INBOUND);
    }

    public static Optional<String> getStopNumber(final String shortName, final int joreDirection) {
        List<String> stopNumbers = getStopNumbers(shortName);
        if (joreDirection < 1 || joreDirection > 2 || stopNumbers.isEmpty()) {
            return Optional.empty();
        }
        // The first stop number corresponds Jore direction 1 and the second stop number corresponds Jore direction 2
        String stopNumber;
        try {
            stopNumber = stopNumbers.get(joreDirection - 1);
        } catch (Exception e) {
            return Optional.empty();
        }
        return Optional.ofNullable(stopNumber);
    }

    public static List<String> getStopNumbers(final String shortName) {
        return stopNumbersByShortName.get(shortName);
    }

    public static Optional<String> convertMetroAtsDatetimeToUtcDatetime(final String metroAtsDatetime) {
        return convertDatetime(metroAtsDatetime, metroAtsDateTimeFormatter, utcZoneId);
    }

    public static Optional<String> convertDatetime(final String datetime, final DateTimeFormatter formatter, final ZoneId toZoneId) {
        if (datetime == null || datetime.isEmpty() || datetime.equals("null")) {
            return Optional.empty();
        }

        try {
            final ZonedDateTime zonedDateTime = ZonedDateTime.parse(datetime, formatter).withZoneSameInstant(toZoneId);
            return Optional.of(zonedDateTime.format(dateTimeFormatter));
        } catch (Exception e) {
            log.error(String.format("Failed to parse datetime from %s", datetime), e);
            return Optional.empty();
        }
    }

    public static Optional<String> convertUtcDatetimeToPubtransDatetime(final String utcDatetime) {
        return convertDatetime(utcDatetime, utcDateTimeFormatter, pubtransZoneId);
    }
}
