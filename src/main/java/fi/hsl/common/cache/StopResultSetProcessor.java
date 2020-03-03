package fi.hsl.common.cache;

import org.slf4j.*;

import java.sql.*;

public class StopResultSetProcessor extends AbstractResultSetProcessor {

    private static final Logger log = LoggerFactory.getLogger(StopResultSetProcessor.class);

    public StopResultSetProcessor(final RedisUtils redisUtils, final QueryUtils queryUtils) {
        super(redisUtils, queryUtils);
    }

    public void processResultSet(final ResultSet resultSet) throws Exception {
        int rowCounter = 0;
        int redisCounter = 0;

        while (resultSet.next()) {
            rowCounter++;
            String key = fi.hsl.common.transitdata.TransitdataProperties.REDIS_PREFIX_JPP + resultSet.getString("Gid");
            String response = redisUtils.setValue(key, resultSet.getString("Number"));
            if (redisUtils.checkResponse(response)) {
                redisCounter++;
            } else {
                log.error("Failed to set stop key {}, Redis returned {}", key, response);
            }
        }

        log.info("Inserted {} redis stop id keys (jpp-id) for {} DB rows", redisCounter, rowCounter);
    }

    protected String getQuery() {
        return new StringBuilder()
                .append("SELECT ")
                .append("[Gid], [Number] ")
                .append("FROM [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP ")
                .append("GROUP BY JPP.Gid, JPP.Number ")
                .toString();
    }
}
