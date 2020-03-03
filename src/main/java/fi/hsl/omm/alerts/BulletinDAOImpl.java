package fi.hsl.omm.alerts;


import fi.hsl.common.transitdata.proto.*;
import fi.hsl.omm.alerts.models.*;

import java.sql.*;
import java.util.*;
import java.util.stream.*;

public class BulletinDAOImpl extends DAOImplBase implements BulletinDAO {

    String queryString;
    String timezone;
    int pollIntervalInSeconds;
    boolean queryAllModifiedAlerts;

    public BulletinDAOImpl(Connection connection, String timezone, int pollIntervalInSeconds, boolean queryAllModifiedAlerts) {
        super(connection);
        this.timezone = timezone;
        this.pollIntervalInSeconds = pollIntervalInSeconds;
        this.queryAllModifiedAlerts = queryAllModifiedAlerts;
        queryString = createQuery(queryAllModifiedAlerts);
    }

    private String createQuery(boolean queryAllModifiedAlerts) {
        return "SELECT B.bulletins_id" +
                "    ,PBMD.impact" +
                "    ,B.category" +
                "    ,B.last_modified" +
                "    ,B.valid_from" +
                "    ,B.valid_to" +
                "    ,B.affects_all_routes" +
                "    ,B.affects_all_stops" +
                "    ,B.affected_route_ids" +
                "    ,B.affected_stop_ids" +
                "    ,PBMD.priority" +
                "    ,MAX(CASE WHEN BLM.language_code = 'fi' THEN BLM.title END) AS title_fi" +
                "    ,MAX(CASE WHEN BLM.language_code = 'sv' THEN BLM.title END) AS title_sv" +
                "    ,MAX(CASE WHEN BLM.language_code = 'en' THEN BLM.title END) AS title_en" +
                "    ,MAX(CASE WHEN BLM.language_code = 'fi' THEN BLM.description END) AS text_fi" +
                "    ,MAX(CASE WHEN BLM.language_code = 'sv' THEN BLM.description END) AS text_sv" +
                "    ,MAX(CASE WHEN BLM.language_code = 'en' THEN BLM.description END) AS text_en" +
                "    ,MAX(CASE WHEN BLM.language_code = 'fi' THEN BLM.url END) AS url_fi" +
                "    ,MAX(CASE WHEN BLM.language_code = 'sv' THEN BLM.url END) AS url_sv" +
                "    ,MAX(CASE WHEN BLM.language_code = 'en' THEN BLM.url END) AS url_en" +
                "  FROM [OMM_Community].[dbo].[bulletins] AS B" +
                "    LEFT JOIN [OMM_Community].[dbo].bulletin_localized_messages AS BLM ON BLM.bulletins_id = B.bulletins_id" +
                "    LEFT JOIN [OMM_Community].[dbo].passenger_bulletin_meta_data AS PBMD ON PBMD.bulletins_id = B.bulletins_id" +
                "  WHERE B.[type] = 'PASSENGER_INFORMATION' " +
                "    AND B.valid_to > ?" + (queryAllModifiedAlerts == true ? " OR B.last_modified > ?" : "") +
                "  GROUP BY B.bulletins_id" +
                "    ,B.category" +
                "    ,PBMD.impact" +
                "    ,B.last_modified" +
                "    ,B.valid_from" +
                "    ,B.valid_to" +
                "    ,B.affects_all_routes" +
                "    ,B.affects_all_stops" +
                "    ,B.affected_route_ids" +
                "    ,B.affected_stop_ids" +
                "    ,PBMD.priority";
    }

    @Override
    public List<Bulletin> getActiveBulletins() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(queryString)) {
            String now = localDatetimeAsString(timezone);
            statement.setString(1, now);
            if (queryAllModifiedAlerts) {
                String pastNow = pastLocalDatetimeAsString(timezone, pollIntervalInSeconds);
                statement.setString(2, pastNow);
            }

            ResultSet results = performQuery(statement);
            return parseBulletins(results);
        } catch (Exception e) {
            log.error("Error while  querying and processing Routes", e);
            throw e;
        }
    }

    private List<Bulletin> parseBulletins(ResultSet resultSet) throws SQLException {
        List<Bulletin> bulletins = new LinkedList<>();
        while (resultSet.next()) {
            try {
                Bulletin bulletin = new Bulletin();
                bulletin.setId(resultSet.getLong("bulletins_id"));
                log.debug("Handling bulletin id {}", bulletin.getId());
                bulletin.setCategory(Bulletin.Category.fromString(resultSet.getString("category")));
                bulletin.setImpact(Bulletin.Impact.fromString(resultSet.getString("impact")));
                bulletin.setLastModified(parseOmmLocalDateTime(resultSet.getString("last_modified")));
                bulletin.setValidFrom(parseNullableOmmLocalDateTime(resultSet.getString("valid_from")));
                bulletin.setValidTo(parseNullableOmmLocalDateTime(resultSet.getString("valid_to")));
                bulletin.setAffectsAllRoutes(resultSet.getInt("affects_all_routes") > 0);
                bulletin.setAffectsAllStops(resultSet.getInt("affects_all_stops") > 0);
                bulletin.setAffectedLineGids(parseListFromCommaSeparatedString(resultSet.getString("affected_route_ids")));
                bulletin.setAffectedStopGids(parseListFromCommaSeparatedString(resultSet.getString("affected_stop_ids")));
                bulletin.setTitles(parseTitles(resultSet));
                bulletin.setDescriptions(parseDescriptions(resultSet));
                bulletin.setUrls(parseUrls(resultSet));

                final Integer priorityInt = resultSet.getInt("priority");
                Optional<Bulletin.Priority> maybePriority = Bulletin.Priority.fromInt(priorityInt);
                if (maybePriority.isPresent()) {
                    bulletin.setPriority(maybePriority.get());
                    bulletins.add(bulletin);
                } else {
                    // Do not handle bulletin if priority cannot be parsed
                    log.warn("Failed to parse priority {}", priorityInt);
                }
            } catch (IllegalArgumentException iae) {
                log.error("Failed to create bulletin because of unexpected data: ", iae);
            }
        }
        return bulletins;
    }

    static List<Long> parseListFromCommaSeparatedString(String value) {
        if (value != null && !value.isEmpty()) {
            return Arrays.stream(value.split(","))
                    .map(subString -> Long.parseLong(subString.trim()))
                    .collect(Collectors.toList());
        } else {
            return new LinkedList<>();
        }
    }

    static List<InternalMessages.Bulletin.Translation> parseTitles(ResultSet resultSet) throws SQLException {
        return parseText(resultSet, "title_");
    }

    static List<InternalMessages.Bulletin.Translation> parseDescriptions(ResultSet resultSet) throws SQLException {
        return parseText(resultSet, "text_");
    }

    static List<InternalMessages.Bulletin.Translation> parseUrls(ResultSet resultSet) throws SQLException {
        return parseText(resultSet, "url_");
    }

    private static List<InternalMessages.Bulletin.Translation> parseText(final ResultSet resultSet, final String columnPrefix) throws SQLException {
        List<InternalMessages.Bulletin.Translation> translations = new ArrayList<>();

        String[] suffixes = {Bulletin.Language.en.toString(), Bulletin.Language.fi.toString(), Bulletin.Language.sv.toString()};
        for (String language : suffixes) {
            String text = resultSet.getString(columnPrefix + language);
            if (text != null) {
                InternalMessages.Bulletin.Translation translation = InternalMessages.Bulletin.Translation.newBuilder()
                        .setText(text)
                        .setLanguage(language).build();

                translations.add(translation);
            }
        }
        return translations;
    }
}
