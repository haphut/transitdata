package fi.hsl.omm.alerts;

import org.slf4j.*;

import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class DAOImplBase {
    static final Logger log = LoggerFactory.getLogger(DAOImplBase.class);
    private static final DateTimeFormatter OMM_DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    protected Connection connection;

    DAOImplBase(Connection connection) {
        this.connection = connection;
    }

    static String localDateAsString(String zoneId) {
        return localDateAsString(Instant.now(), zoneId);
    }

    private static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

    static String localDatetimeAsString(String zoneId) {
        return localDatetimeAsString(Instant.now(), zoneId);
    }

    private static String localDatetimeAsString(Instant instant, String zoneId) {
        return OMM_DT_FORMATTER.format(instant.atZone(ZoneId.of(zoneId)));
    }

    static String pastLocalDatetimeAsString(String zoneId, int intervalSecs) {
        Instant pastNow = Instant.now().minusSeconds(intervalSecs);
        return localDatetimeAsString(pastNow, zoneId);
    }

    static Optional<LocalDateTime> parseNullableOmmLocalDateTime(String dt) {
        if (dt != null) {
            return Optional.of(parseOmmLocalDateTime(dt));
        } else {
            return Optional.empty();
        }
    }

    public static LocalDateTime parseOmmLocalDateTime(String dt) {
        return LocalDateTime.parse(dt.replace(" ", "T")); // Make java.sql.Timestamp ISO compatible
    }

    ResultSet performQuery(PreparedStatement statement) throws SQLException {
        long queryStartTime = System.currentTimeMillis();
        ResultSet resultSet = statement.executeQuery();
        long elapsed = System.currentTimeMillis() - queryStartTime;
        log.info("Total query and processing time: {} ms", elapsed);
        return resultSet;
    }
}
