package fi.hsl.omm.alerts;

import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import fi.hsl.common.transitdata.proto.*;
import fi.hsl.omm.alerts.models.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;

import java.time.*;
import java.util.*;
import java.util.stream.*;

public class OmmAlertHandler {

    public static final String AGENCY_ENTITY_SELECTOR = "HSL";

    static final Logger log = LoggerFactory.getLogger(OmmAlertHandler.class);
    private final Producer<byte[]> producer;
    String timeZone;
    OmmDbConnector ommConnector;
    private AlertState previousState = null;
    private Map<Long, Line> lines = null;
    private LocalDate linesUpdateDate = null;

    public OmmAlertHandler(PulsarApplicationContext context, OmmDbConnector omm) {
        producer = context.getProducer();
        timeZone = context.getConfig().getString("fi.hsl.omm.timezone");
        ommConnector = omm;
    }

    public void pollAndSend() throws Exception {
        try {
            //For some reason the connection seem to be flaky, let's reconnect on each request.
            ommConnector.connect();

            BulletinDAO bulletinDAO = ommConnector.getBulletinDAO();
            LineDAO lineDAO = ommConnector.getLineDAO();
            StopPointDAO stopPointDAO = ommConnector.getStopPointDAO();

            List<Bulletin> bulletins = bulletinDAO.getActiveBulletins();
            AlertState latestState = new AlertState(bulletins);

            if (!LocalDate.now().equals(linesUpdateDate)) {
                lines = lineDAO.getAllLines();
                linesUpdateDate = LocalDate.now();
            }

            if (!latestState.equals(previousState)) {
                log.info("Service Alerts changed, creating new FeedMessage.");

                Map<Long, List<StopPoint>> stopPoints = stopPointDAO.getAllStopPoints();

                // We want to keep Pulsar internal timestamps as accurate as possible (ms) but GTFS-RT expects milliseconds
                final long currentTimestampUtcMs = toUtcEpochMs(LocalDateTime.now(), timeZone);

                final InternalMessages.ServiceAlert alert = createServiceAlert(bulletins, lines, stopPoints, timeZone);
                sendPulsarMessage(alert, currentTimestampUtcMs);
            } else {
                log.info("No changes to current Service Alerts.");
            }
            previousState = latestState;
        } finally {
            ommConnector.close();
        }
    }

    private static long toUtcEpochMs(final LocalDateTime localTimestamp, final String zoneId) {
        ZoneId zone = ZoneId.of(zoneId);
        return localTimestamp.atZone(zone).toInstant().toEpochMilli();
    }

    static InternalMessages.ServiceAlert createServiceAlert(final List<Bulletin> bulletins, final Map<Long, Line> lines, final Map<Long, List<StopPoint>> stopPoints, final String timeZone) {
        final InternalMessages.ServiceAlert.Builder builder = InternalMessages.ServiceAlert.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());
        final List<InternalMessages.Bulletin> internalMessageBulletins = bulletins.stream()
                .map(bulletin -> createBulletin(bulletin, lines, stopPoints, timeZone))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        builder.addAllBulletins(internalMessageBulletins);
        return builder.build();
    }

    private void sendPulsarMessage(final InternalMessages.ServiceAlert message, long timestamp) throws PulsarClientException {
        try {
            producer.newMessage().value(message.toByteArray())
                    .eventTime(timestamp)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString())
                    .send();

            log.info("Produced a new alert of {} bulletins with timestamp {}", message.getBulletinsCount(), timestamp);
        } catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        } catch (Exception e) {
            log.error("Failed to handle alert message", e);
        }
    }

    static Optional<InternalMessages.Bulletin> createBulletin(final Bulletin bulletin, final Map<Long, Line> lines, final Map<Long, List<StopPoint>> stopPoints, final String timezone) {
        Optional<InternalMessages.Bulletin> maybeBulletin;
        try {
            final InternalMessages.Bulletin.Builder builder = InternalMessages.Bulletin.newBuilder();

            builder.setBulletinId(Long.toString(bulletin.getId()));
            builder.setCategory(bulletin.getCategory().toCategory());

            long lastModifiedInUtcMs = toUtcEpochMs(bulletin.getLastModified(), timezone);
            builder.setLastModifiedUtcMs(lastModifiedInUtcMs);

            Optional<Long> startInUtcMs = bulletin.getValidFrom().map(from -> toUtcEpochMs(from, timezone));
            if (startInUtcMs.isPresent()) {
                builder.setValidFromUtcMs(startInUtcMs.get());
            } else {
                log.error("No start time specified for bulletin {}", bulletin.getId());
            }

            Optional<Long> stopInUtcMs = bulletin.getValidTo().map(to -> toUtcEpochMs(to, timezone));
            if (stopInUtcMs.isPresent()) {
                builder.setValidToUtcMs(stopInUtcMs.get());
            } else {
                log.error("No end time specified for bulletin {}", bulletin.getId());
            }

            builder.setAffectsAllRoutes(bulletin.isAffectsAllRoutes());
            builder.setAffectsAllStops(bulletin.isAffectsAllStops());
            builder.setImpact(bulletin.getImpact().toImpact());
            builder.setPriority(bulletin.getPriority().toPriority());

            builder.addAllTitles(bulletin.getTitles());
            builder.addAllDescriptions(bulletin.getDescriptions());
            builder.addAllUrls(bulletin.getUrls());

            List<InternalMessages.Bulletin.AffectedEntity> affectedRoutes = getAffectedRoutes(bulletin, lines);
            List<InternalMessages.Bulletin.AffectedEntity> affectedStops = getAffectedStops(bulletin, stopPoints);
            if (affectedRoutes.isEmpty() && affectedStops.isEmpty() && !bulletin.isAffectsAllRoutes() && !bulletin.isAffectsAllStops()) {
                log.warn("Failed to find any Affected Entities for bulletin Id {}. Discarding alert.", bulletin.getId());
                maybeBulletin = Optional.empty();
            } else {
                builder.addAllAffectedRoutes(affectedRoutes);
                builder.addAllAffectedStops(affectedStops);
                maybeBulletin = Optional.of(builder.build());
            }
        } catch (Exception e) {
            log.error("Exception while creating an alert!", e);
            maybeBulletin = Optional.empty();
        }
        return maybeBulletin;
    }

    static List<InternalMessages.Bulletin.AffectedEntity> getAffectedRoutes(final Bulletin bulletin, final Map<Long, Line> lines) {
        List<InternalMessages.Bulletin.AffectedEntity> affectedRoutes = new LinkedList<>();
        if (bulletin.getAffectedLineGids().size() > 0) {
            for (Long lineGid : bulletin.getAffectedLineGids()) {
                Optional<Line> line = Optional.ofNullable(lines.get(lineGid));
                if (line.isPresent()) {
                    List<Route> routes = line.get().routes;
                    routes = routes.stream()
                            .filter(route -> entityIsTimeValidForBulletin(bulletin, route.getExistsFromDate(), route.getExistsUptoDate()))
                            .collect(Collectors.toList());
                    routes.forEach((route) -> {
                        InternalMessages.Bulletin.AffectedEntity entity = InternalMessages.Bulletin.AffectedEntity.newBuilder()
                                .setEntityId(route.getRouteId()).build();
                        if (!affectedRoutes.contains(entity)) {
                            affectedRoutes.add(entity);
                        }
                    });
                } else {
                    log.error("Failed to find line ID for line GID: {}, bulletin id: {}", lineGid, bulletin.getId());
                }
            }
            log.debug("Found {} entity selectors for routes (should have been {})", affectedRoutes.size(), bulletin.getAffectedLineGids().size());
        }
        return affectedRoutes;
    }

    static List<InternalMessages.Bulletin.AffectedEntity> getAffectedStops(final Bulletin bulletin, final Map<Long, List<StopPoint>> stopsMap) {
        List<InternalMessages.Bulletin.AffectedEntity> affectedStops = new LinkedList<>();
        if (bulletin.getAffectedStopGids().size() > 0) {
            for (Long stopGid : bulletin.getAffectedStopGids()) {
                Optional<List<StopPoint>> stops = Optional.ofNullable(stopsMap.get(stopGid));
                if (stops.isPresent()) {
                    for (StopPoint stopPoint : stops.get()) {
                        if (entityIsTimeValidForBulletin(bulletin, stopPoint.getExistsFromDate(), stopPoint.getExistsUptoDate())) {
                            String stopId = stopPoint.getStopId();
                            InternalMessages.Bulletin.AffectedEntity entity = InternalMessages.Bulletin.AffectedEntity.newBuilder()
                                    .setEntityId(stopId).build();
                            if (!affectedStops.contains(entity)) {
                                affectedStops.add(entity);
                            }
                        }
                    }
                } else {
                    log.warn("Failed to find valid stop ID for stop GID: {}", stopGid);
                }
            }
            log.debug("Found {} entity selectors for routes (should have been {})", affectedStops.size(), bulletin.getAffectedStopGids().size());
        }
        return affectedStops;
    }

    static boolean entityIsTimeValidForBulletin(final Bulletin bulletin, final Optional<LocalDateTime> existsFromDate, final Optional<LocalDateTime> existsUptoDate) {
        boolean valid = true;
        if (bulletin.getValidTo().isPresent() && existsFromDate.isPresent()) {
            if (existsFromDate.get().isAfter(bulletin.getValidTo().get())) {
                valid = false;
            }
        }
        if (bulletin.getValidFrom().isPresent() && existsUptoDate.isPresent()) {
            if (existsUptoDate.get().isBefore(bulletin.getValidFrom().get())) {
                valid = false;
            }
        }
        return valid;
    }
}
