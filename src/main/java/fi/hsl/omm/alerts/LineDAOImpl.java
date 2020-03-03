package fi.hsl.omm.alerts;

import fi.hsl.common.files.*;
import fi.hsl.omm.alerts.models.*;

import java.io.*;
import java.sql.*;
import java.util.*;

public class LineDAOImpl extends DAOImplBase implements LineDAO {

    String queryString;

    LineDAOImpl(Connection connection) {
        super(connection);
        queryString = createQuery();
    }

    private String createQuery() {
        InputStream stream = getClass().getResourceAsStream("/routes_all.sql");
        try {
            return FileUtils.readFileFromStreamOrThrow(stream);
        } catch (Exception e) {
            log.error("Error in reading sql from file:", e);
            return null;
        }
    }

    @Override
    public Map<Long, Line> getAllLines() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(queryString)) {
            log.info("Querying lines");
            ResultSet results = performQuery(statement);
            return parseLines(results);
        } catch (Exception e) {
            log.error("Error while  querying and processing Lines", e);
            throw e;
        }
    }

    private Map<Long, Line> parseLines(ResultSet resultSet) throws SQLException {
        Map<Long, Line> lines = new HashMap<>();
        while (resultSet.next()) {
            long lineGid = resultSet.getLong("Gid");
            String routeId = resultSet.getString("StringValue");
            String existsFromDate = resultSet.getString("ExistsFromDate");
            String existsUptoDate = resultSet.getString("ExistsUptoDate");
            Route route = new Route(lineGid, routeId, existsFromDate, existsUptoDate);
            Line line;
            if (lines.containsKey(lineGid)) {
                line = lines.get(lineGid);
            } else {
                line = new Line(lineGid);
                lines.put(line.getGid(), line);
            }
            line.addRouteToLine(route);
        }
        log.info("Parsed {} lines", lines.size());
        return lines;
    }

}
