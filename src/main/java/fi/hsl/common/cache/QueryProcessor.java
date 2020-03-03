package fi.hsl.common.cache;

import org.slf4j.*;
import redis.clients.jedis.exceptions.*;

import java.sql.*;

public class QueryProcessor {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);

    public Connection connection;

    public QueryProcessor(final Connection connection) {
        this.connection = connection;
    }

    public void executeAndProcessQuery(final AbstractResultSetProcessor processor) {
        final String processorName = processor.getClass().getName();
        log.info("Starting query with result set processor {}.", processorName);
        long now = System.currentTimeMillis();

        ResultSet resultSet = null;
        try {
            final String query = processor.getQuery();
            resultSet = executeQuery(query);
            processor.processResultSet(resultSet);
        } catch (JedisConnectionException e) {
            log.error(String.format("Failed to connect to Redis while running processor %s.", processorName), e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to process query", e);
        } finally {
            closeQuery(resultSet);
        }

        long elapsed = (System.currentTimeMillis() - now) / 1000;
        log.info("Data handled in " + elapsed + " seconds");
    }

    private ResultSet executeQuery(final String query) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        return resultSet;
    }

    private static void closeQuery(final ResultSet resultSet) {
        Statement statement = null;
        try {
            statement = resultSet.getStatement();
        } catch (Exception e) {
            log.error("Failed to get Statement", e);
        }
        if (resultSet != null) try {
            resultSet.close();
        } catch (Exception e) {
            log.error("Failed to close ResultSet", e);
        }
        if (statement != null) try {
            statement.close();
        } catch (Exception e) {
            log.error("Failed to close Statement", e);
        }
    }
}
