package fi.hsl.common.pubtrans;

import com.typesafe.config.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import fi.hsl.tripupdate.arrival.*;
import fi.hsl.tripupdate.departure.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;
import redis.clients.jedis.*;

import java.sql.Connection;
import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.time.temporal.*;
import java.util.*;

public class PubtransConnector {

    private static final Logger log = LoggerFactory.getLogger(PubtransConnector.class);

    private Connection connection;
    private long queryStartTime;

    private String queryString;
    private boolean enableCacheCheck;
    private int cacheMaxAgeInMins;

    private PubtransTableHandler handler;
    private Jedis jedis;
    private Producer<byte[]> producer;

    private PubtransConnector() {
    }

    public static PubtransConnector newInstance(Connection connection,
                                                PulsarApplicationContext context,
                                                PubtransTableType tableType) throws RuntimeException {
        PubtransConnector connector = new PubtransConnector();

        connector.connection = connection;
        connector.jedis = context.getJedis();
        connector.producer = context.getProducer();

        Config config = context.getConfig();
        connector.queryString = queryString(config);
        connector.enableCacheCheck = config.getBoolean("application.enableCacheTimestampCheck");
        connector.cacheMaxAgeInMins = config.getInt("application.cacheMaxAgeInMinutes");

        log.info("Cache pre-condition enabled: " + connector.enableCacheCheck + " with max age " + connector.cacheMaxAgeInMins);

        log.info("TableType: " + tableType);
        switch (tableType) {
            case ROI_ARRIVAL:
                connector.handler = new ArrivalHandler(context);
                break;
            case ROI_DEPARTURE:
                connector.handler = new DepartureHandler(context);
                break;
            default:
                throw new IllegalArgumentException("Table type not supported");
        }
        return connector;
    }

    private static String queryString(Config config) {
        String longName = config.getString("pubtrans.longName");
        String shortName = config.getString("pubtrans.shortName");

        return "SELECT * FROM " +
                longName +
                " AS " +
                shortName +
                " WHERE " +
                shortName + ".LastModifiedUTCDateTime > ? " +
                " ORDER BY " +
                shortName + ".LastModifiedUTCDateTime, " +
                shortName + ".IsOnDatedVehicleJourneyId, " +
                shortName + ".JourneyPatternSequenceNumber DESC";
    }

    public boolean checkPrecondition() {
        if (!enableCacheCheck)
            return true;
        synchronized (jedis) {
            String lastUpdate = jedis.get(TransitdataProperties.KEY_LAST_CACHE_UPDATE_TIMESTAMP);
            if (lastUpdate != null) {
                OffsetDateTime dt = OffsetDateTime.parse(lastUpdate, DateTimeFormatter.ISO_DATE_TIME);
                return isCacheValid(dt, cacheMaxAgeInMins);
            } else {
                log.error("Could not find last cache update timestamp from redis");
                return false;
            }
        }
    }

    private static boolean isCacheValid(OffsetDateTime lastCacheUpdate, final int cacheMaxAgeInMins) {

        OffsetDateTime now = OffsetDateTime.now();
        //Java8 does not support getting duration as minutes directly.
        final long secondsSinceUpdate = Duration.between(lastCacheUpdate, now).get(ChronoUnit.SECONDS);
        final long minutesSinceUpdate = Math.floorDiv(secondsSinceUpdate, 60);
        log.debug("Current time " + now.toString() + ", last update " + lastCacheUpdate.toString() + " => mins from prev update: " + minutesSinceUpdate);
        return minutesSinceUpdate <= cacheMaxAgeInMins;
    }

    public void queryAndProcessResults() throws SQLException, PulsarClientException {

        queryStartTime = System.currentTimeMillis();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.prepareStatement(queryString);
            statement.setTimestamp(1, new Timestamp(handler.getLastModifiedTimeStamp()));
            resultSet = statement.executeQuery();
            produceMessages(handler.handleResultSet(resultSet));
        } finally {
            if (resultSet != null) try {
                resultSet.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            if (statement != null) try {
                statement.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    private void produceMessages(Queue<TypedMessageBuilder<byte[]>> messageBuilderQueue) throws PulsarClientException {
        if (!producer.isConnected()) {
            throw new PulsarClientException("Producer is not connected");
        }

        for (TypedMessageBuilder<byte[]> msg : messageBuilderQueue) {
            msg.sendAsync()
                    .exceptionally(throwable -> {
                        log.error("Failed to send Pulsar message", throwable);
                        return null;
                    });

        }
        //If we want to get Pulsar Exceptions to bubble up into this thread we need to do a sync flush for all pending messages.
        producer.flush();

        log.info(messageBuilderQueue.size() + " messages written. Latest timestamp: " + handler.getLastModifiedTimeStamp() +
                " Total query and processing time: " + (System.currentTimeMillis() - this.queryStartTime) + " ms");
    }
}

