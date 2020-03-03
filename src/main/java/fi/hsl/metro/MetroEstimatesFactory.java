package fi.hsl.metro;

import com.fasterxml.jackson.databind.*;
import fi.hsl.common.mqtt.proto.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import fi.hsl.common.transitdata.proto.*;
import fi.hsl.metro.models.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;
import redis.clients.jedis.*;

import java.util.*;

class MetroEstimatesFactory {

    private static final Logger log = LoggerFactory.getLogger(MetroEstimatesFactory.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private Jedis jedis;

    MetroEstimatesFactory(final PulsarApplicationContext context) {
        this.jedis = context.getJedis();
    }

    Optional<MetroAtsProtos.MetroEstimate> toMetroEstimate(final Message message) {
        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(message);
            if (maybeSchema.isPresent()) {
                final byte[] data = message.getData();
                final Mqtt.RawMessage mqttMessage = Mqtt.RawMessage.parseFrom(data);
                final byte[] payload = mqttMessage.getPayload().toByteArray();
                Optional<MetroEstimate> maybeMetroEstimate = parsePayload(payload);

                if (maybeMetroEstimate.isPresent()) {
                    final MetroEstimate metroEstimate = maybeMetroEstimate.get();
                    return toMetroEstimate(metroEstimate);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to produce metro schedule stop estimates.", e);
        }
        return Optional.empty();
    }

    private static Optional<MetroEstimate> parsePayload(final byte[] payload) {
        try {
            MetroEstimate metroEstimate = mapper.readValue(payload, MetroEstimate.class);
            return Optional.of(metroEstimate);
        } catch (Exception e) {
            log.warn(String.format("Failed to parse payload %s.", new String(payload)), e);
        }
        return Optional.empty();
    }

    private Optional<MetroAtsProtos.MetroEstimate> toMetroEstimate(final MetroEstimate metroEstimate) {
        final String[] stopShortNames = metroEstimate.routeName.split("-");
        if (stopShortNames.length != 2) {
            log.warn("Failed to parse metro estimate route name {}", metroEstimate.routeName);
            return Optional.empty();
        }
        final String startStopShortName = stopShortNames[0];
        final String endStopShortName = stopShortNames[1];

        // Create a metroKey to be used for Redis Query
        String metroKey;
        Optional<String> maybeStopNumber = MetroUtils.getStopNumber(startStopShortName, startStopShortName, endStopShortName);
        if (maybeStopNumber.isPresent()) {
            String stopNumber = maybeStopNumber.get();
            // Convert UTC datetime to local datetime because the keys in Redis have local datetime
            final Optional<String> maybeStartDatetime = MetroUtils.convertUtcDatetimeToPubtransDatetime(metroEstimate.getBeginTime());
            if (maybeStartDatetime.isPresent()) {
                final String startDatetime = maybeStartDatetime.get();
                metroKey = TransitdataProperties.formatMetroId(stopNumber, startDatetime);
            } else {
                log.warn("Failed to convert UTC datetime {} to local datetime", metroEstimate.getBeginTime());
                return Optional.empty();
            }
        } else {
            log.warn("Failed to get stop number for stop shortNames: startStopShortName: {}, endStopShortName: {}", startStopShortName, endStopShortName);
            return Optional.empty();
        }
        MetroAtsProtos.MetroEstimate.Builder metroEstimateBuilder = MetroAtsProtos.MetroEstimate.newBuilder();

        // Set fields from mqtt-pulsar-gateway into metroEstimateBuilder
        metroEstimateBuilder.setSchemaVersion(metroEstimateBuilder.getSchemaVersion());
        // trainType
        Optional<MetroAtsProtos.MetroTrainType> maybeMetroTrainTypeAts = getMetroTrainTypeAts(metroEstimate.trainType);
        if (maybeMetroTrainTypeAts.isEmpty()) {
            log.warn("metroEstimate.trainType is missing: {}", metroEstimate.trainType);
            return Optional.empty();
        }
        metroEstimateBuilder.setTrainType(maybeMetroTrainTypeAts.get());
        // journeySectionprogress
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress = getMetroAtsProgress(metroEstimate.journeySectionprogress);
        if (maybeMetroAtsProgress.isEmpty()) {
            log.warn("metroEstimate.journeySectionprogress is missing: {}", metroEstimate.journeySectionprogress);
            return Optional.empty();
        }
        metroEstimateBuilder.setJourneySectionprogress(maybeMetroAtsProgress.get());
        metroEstimateBuilder.setBeginTime(metroEstimate.getBeginTime());
        metroEstimateBuilder.setEndTime(metroEstimate.getEndTime());
        metroEstimateBuilder.setStartStopShortName(startStopShortName);

        // Set fields from Redis into metroEstimateBuilder
        Optional<Map<String, String>> metroJourneyData = getMetroJourneyData(metroKey);
        if (metroJourneyData.isEmpty()) {
            log.warn("Couldn't read metroJourneyData from redis. Metro key: {}, redis map: {}.", metroKey, metroJourneyData);
            return Optional.empty();
        }
        metroJourneyData.ifPresent(map -> {
            if (map.containsKey(TransitdataProperties.KEY_OPERATING_DAY))
                metroEstimateBuilder.setOperatingDay(map.get(TransitdataProperties.KEY_OPERATING_DAY));
            if (map.containsKey(TransitdataProperties.KEY_START_STOP_NUMBER))
                metroEstimateBuilder.setStartStopNumber(map.get(TransitdataProperties.KEY_START_STOP_NUMBER));
            if (map.containsKey(TransitdataProperties.KEY_START_TIME))
                metroEstimateBuilder.setStartTime(map.get(TransitdataProperties.KEY_START_TIME));
            if (map.containsKey(TransitdataProperties.KEY_DVJ_ID))
                metroEstimateBuilder.setDvjId(map.get(TransitdataProperties.KEY_DVJ_ID));
            if (map.containsKey(TransitdataProperties.KEY_ROUTE_NAME))
                metroEstimateBuilder.setRouteName(map.get(TransitdataProperties.KEY_ROUTE_NAME));
            if (map.containsKey(TransitdataProperties.KEY_START_DATETIME))
                metroEstimateBuilder.setStartDatetime(map.get(TransitdataProperties.KEY_START_DATETIME));
            if (map.containsKey(TransitdataProperties.KEY_DIRECTION))
                metroEstimateBuilder.setDirection(map.get(TransitdataProperties.KEY_DIRECTION));
        });

        Optional<String> directionString = Optional.of(metroJourneyData.get().get(TransitdataProperties.KEY_DIRECTION));
        if (directionString.isEmpty()) {
            log.warn("Couldn't read directionString from metroJourneyData: {}.", directionString);
            return Optional.empty();
        }
        Integer direction = Integer.parseInt(directionString.get());

        // routeRows
        List<MetroAtsProtos.MetroStopEstimate> metroStopEstimates = new ArrayList<>();
        for (MetroStopEstimate metroStopEstimate : metroEstimate.routeRows) {
            Optional<MetroAtsProtos.MetroStopEstimate> maybeMetroStopEstimate = toMetroStopEstimate(metroStopEstimate, direction, metroEstimate.getBeginTime(), startStopShortName);
            if (maybeMetroStopEstimate.isEmpty()) {
                return Optional.empty();
            } else {
                metroStopEstimates.add(maybeMetroStopEstimate.get());
            }
        }

        if (!metroStopEstimates.stream().map(MetroAtsProtos.MetroStopEstimate::getStopNumber).allMatch(new HashSet<>()::add)) {
            log.warn("Metro {} (beginTime: {}, dir: {}) had multiple estimates for one stop", metroEstimate.routeName, metroEstimate.getBeginTime(), direction);
        }

        metroEstimateBuilder.addAllMetroRows(metroStopEstimates);

        return Optional.of(metroEstimateBuilder.build());
    }

    private Optional<MetroAtsProtos.MetroTrainType> getMetroTrainTypeAts(MetroTrainType metroTrainType) {
        Optional<MetroAtsProtos.MetroTrainType> maybeMetroTrainTypeAts;
        switch (metroTrainType) {
            case M:
                maybeMetroTrainTypeAts = Optional.of(MetroAtsProtos.MetroTrainType.M);
                break;
            case T:
                maybeMetroTrainTypeAts = Optional.of(MetroAtsProtos.MetroTrainType.T);
                break;
            default:
                log.warn("Unrecognized metroTrainType {}.", metroTrainType);
                maybeMetroTrainTypeAts = Optional.empty();
                break;
        }

        return maybeMetroTrainTypeAts;
    }

    private Optional<MetroAtsProtos.MetroProgress> getMetroAtsProgress(MetroProgress metroProgress) {
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress;
        switch (metroProgress) {
            case SCHEDULED:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.SCHEDULED);
                break;
            case INPROGRESS:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.INPROGRESS);
                break;
            case COMPLETED:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.COMPLETED);
                break;
            case CANCELLED:
                maybeMetroAtsProgress = Optional.of(MetroAtsProtos.MetroProgress.CANCELLED);
                break;
            default:
                log.warn("Unrecognized metroProgress {}.", metroProgress);
                maybeMetroAtsProgress = Optional.empty();
                break;
        }
        return maybeMetroAtsProgress;
    }

    private Optional<Map<String, String>> getMetroJourneyData(final String metroKey) {
        synchronized (jedis) {
            try {
                Map<String, String> redisMap = jedis.hgetAll(metroKey);
                if (redisMap.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.ofNullable(redisMap);
            } catch (Exception e) {
                log.error("Couldn't read metroJourneyData from redis. Metro key: {}", metroKey, e);
                return Optional.empty();
            }
        }
    }

    private Optional<MetroAtsProtos.MetroStopEstimate> toMetroStopEstimate(MetroStopEstimate metroStopEstimate, Integer direction, String beginTime, String startStopShortName) {
        MetroAtsProtos.MetroStopEstimate.Builder metroStopEstimateBuilder = MetroAtsProtos.MetroStopEstimate.newBuilder();

        // Set fields from mqtt-pulsar-gateway into metroStopEstimateBuilder
        metroStopEstimateBuilder.setStation((metroStopEstimate.getStation()));
        metroStopEstimateBuilder.setPlatform((metroStopEstimate.getPlatform()));

        if (validateDatetime(metroStopEstimate.getArrivalTimePlanned())) {
            metroStopEstimateBuilder.setArrivalTimePlanned(metroStopEstimate.getArrivalTimePlanned());
        } else {
            metroStopEstimateBuilder.setArrivalTimePlanned("");
        }
        if (validateDatetime(metroStopEstimate.getArrivalTimeForecast())) {
            metroStopEstimateBuilder.setArrivalTimeForecast(metroStopEstimate.getArrivalTimeForecast());
        } else {
            metroStopEstimateBuilder.setArrivalTimeForecast("");
        }
        if (validateDatetime(metroStopEstimate.getArrivalTimeMeasured())) {
            metroStopEstimateBuilder.setArrivalTimeMeasured(metroStopEstimate.getArrivalTimeMeasured());
        } else {
            metroStopEstimateBuilder.setArrivalTimeMeasured("");
        }
        if (validateDatetime(metroStopEstimate.getDepartureTimePlanned())) {
            metroStopEstimateBuilder.setDepartureTimePlanned(metroStopEstimate.getDepartureTimePlanned());
        } else {
            metroStopEstimateBuilder.setDepartureTimePlanned("");
        }
        if (validateDatetime(metroStopEstimate.getDepartureTimeForecast())) {
            metroStopEstimateBuilder.setDepartureTimeForecast(metroStopEstimate.getDepartureTimeForecast());
        } else {
            metroStopEstimateBuilder.setDepartureTimeForecast("");
        }
        if (validateDatetime(metroStopEstimate.getDepartureTimeMeasured())) {
            metroStopEstimateBuilder.setDepartureTimeMeasured(metroStopEstimate.getDepartureTimeMeasured());
        } else {
            metroStopEstimateBuilder.setDepartureTimeMeasured("");
        }
        metroStopEstimateBuilder.setSource(metroStopEstimate.getSource());

        // stop number
        String shortName = metroStopEstimate.getStation();
        Optional<String> maybeStopNumber = MetroUtils.getStopNumber(shortName, direction);
        if (!maybeStopNumber.isPresent()) {
            log.warn("Couldn't find stopNumber for shortName: {} (Metro: direction {}, beginTime {}, startStopShortName: {})", shortName, direction, beginTime, startStopShortName);
            return Optional.empty();
        }
        metroStopEstimateBuilder.setStopNumber(maybeStopNumber.get());

        // rowProgress
        Optional<MetroAtsProtos.MetroProgress> maybeMetroAtsProgress = getMetroAtsProgress(metroStopEstimate.getRowProgress());
        maybeMetroAtsProgress.ifPresent(metroStopEstimateBuilder::setRowProgress);

        return Optional.of(metroStopEstimateBuilder.build());
    }

    private boolean validateDatetime(final String datetime) {
        return datetime != null && !datetime.equals("null") && !datetime.isEmpty();
    }
}
