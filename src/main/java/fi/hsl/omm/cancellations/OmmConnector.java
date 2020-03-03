package fi.hsl.omm.cancellations;

import fi.hsl.common.files.*;
import fi.hsl.common.pulsar.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;

import java.io.*;
import java.sql.*;
import java.time.*;
import java.time.format.*;

public class OmmConnector {

    private static final Logger log = LoggerFactory.getLogger(OmmConnector.class);

    private final Connection dbConnection;
    private final String queryString;
    private final CancellationSourceType sourceType;
    private final String timezone;
    private OmmCancellationHandler handler;

    private OmmConnector(PulsarApplicationContext context, Connection connection, CancellationSourceType type) {
        handler = new OmmCancellationHandler(context);
        dbConnection = connection;
        queryString = createQuery(type);
        sourceType = type;
        timezone = context.getConfig().getString("omm.timezone");
        log.info("Using timezone " + timezone);
    }

    private String createQuery(CancellationSourceType sourceType) {
        InputStream stream = (sourceType == CancellationSourceType.FROM_PAST)
                ? getClass().getResourceAsStream("/cancellations_past_current_future.sql")
                : (sourceType == CancellationSourceType.FROM_NOW)
                ? getClass().getResourceAsStream("/cancellations_current_future.sql")
                : null;
        try {
            return FileUtils.readFileFromStreamOrThrow(stream);
        } catch (Exception e) {
            log.error("Error in reading sql from file:", e);
            return null;
        }
    }

    public static OmmConnector newInstance(PulsarApplicationContext context, String jdbcConnectionString, CancellationSourceType sourceType) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmConnector(context, connection, sourceType);
    }

    public void queryAndProcessResults(int pollIntervalInSeconds) throws SQLException, PulsarClientException {
        //Let's use Strings in the query since JDBC driver tends to convert timestamps automatically to local jvm time.
        Instant now = Instant.now();
        String nowDateTime = localDatetimeAsString(now, timezone);
        String nowDate = localDateAsString(now, timezone);

        log.info("Querying results from database with timestamp {}", now);
        long queryStartTime = System.currentTimeMillis();

        log.trace("Running query " + queryString);

        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            statement.setString(1, nowDateTime);
            statement.setString(2, nowDate);
            if (sourceType == CancellationSourceType.FROM_PAST) {
                Instant pastNow = now.minusSeconds(pollIntervalInSeconds);
                String pastDateTime = localDatetimeAsString(pastNow, timezone);
                statement.setString(3, nowDateTime);
                statement.setString(4, nowDate);
                statement.setString(5, pastDateTime);
            }

            ResultSet resultSet = statement.executeQuery();
            handler.handleAndSend(resultSet);

            long elapsed = System.currentTimeMillis() - queryStartTime;
            if (elapsed > 4000) {
                log.warn("Slow querying & handling of cancellations. Total query and processing time was: {} ms", elapsed);
            }
        } catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    static String localDatetimeAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(instant.atZone(ZoneId.of(zoneId)));
    }

    static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

}
