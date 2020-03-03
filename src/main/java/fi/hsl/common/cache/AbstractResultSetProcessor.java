package fi.hsl.common.cache;

import java.sql.*;

public abstract class AbstractResultSetProcessor {
    RedisUtils redisUtils;
    QueryUtils queryUtils;

    AbstractResultSetProcessor(final RedisUtils redisUtils, final QueryUtils queryUtils) {
        this.redisUtils = redisUtils;
        this.queryUtils = queryUtils;
    }

    public abstract void processResultSet(final ResultSet resultSet) throws Exception;

    protected abstract String getQuery();
}
