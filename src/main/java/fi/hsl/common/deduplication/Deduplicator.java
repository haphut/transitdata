package fi.hsl.common.deduplication;

import avro.shaded.com.google.common.cache.*;
import fi.hsl.common.hfp.proto.*;
import fi.hsl.common.mqtt.proto.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.com.google.common.hash.*;
import org.slf4j.*;

import java.time.*;
import java.util.*;

import static java.util.Optional.*;

public class Deduplicator implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(Deduplicator.class);
    final int SEED = 42; //Let's use a static seed in case we want to store hashes in a more persistent storage at some point (f.ex Redis)
    private final Cache<HashCode, Long> hashCache;
    private final HashFunction hashFunction = Hashing.murmur3_128(SEED);
    private final Optional<Analytics> analytics;
    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    public Deduplicator(PulsarApplicationContext context, Analytics analytics) {
        consumer = context.getConsumer();
        producer = context.getProducer();
        this.analytics = ofNullable(analytics);

        Duration ttl = context.getConfig().getDuration("application.cacheTTL");
        hashCache = CacheBuilder.newBuilder()
                .initialCapacity(35000)
                .maximumSize(30000)
                .build();
    }

    public void handleMessage(Message received) throws Exception {
        try {
            byte[] data = parsePayload(received);
            HashCode hash = hashFunction.hashBytes(data);
            Long cacheHit = hashCache.getIfPresent(hash);
            if (cacheHit == null) {
                // We haven't yet received this so save to cache and send the message.
                // Timestamp is for analytics & debugging purposes
                hashCache.put(hash, System.currentTimeMillis());
                sendPulsarMessage(received);
                analytics.ifPresent(a -> a.reportPrime());
            } else {
                long elapsedSinceHit = System.currentTimeMillis() - cacheHit;
                analytics.ifPresent(a -> a.reportDuplicate(elapsedSinceHit));
            }
            ack(received.getMessageId());
        } catch (Exception e) {
            analytics.ifPresent(a -> a.calcStats());
            log.error("Exception while handling message, aborting", e);
            throw e;
        }
    }

    /**
     * Because protobuf is not deterministic in how it orders the bytes, we need to de-serialize the payload first.
     */
    private byte[] parsePayload(Message received) {
        final byte[] sourceData = received.getData();
        Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(received);

        Optional<byte[]> mappedData = maybeSchema
                .filter(transitdataSchema ->
                        //We only support these two protobuf formats. Add others if needed:
                        transitdataSchema.schema.equals(TransitdataProperties.ProtobufSchema.MqttRawMessage) ||
                                transitdataSchema.schema.equals(TransitdataProperties.ProtobufSchema.HfpData)
                ).flatMap(
                        transitdataSchema -> {
                            try {
                                return Optional.of(parsePayload(transitdataSchema.schema, sourceData));
                            } catch (Exception e) {
                                log.error("Could not parse expected protobuf schema: {}", transitdataSchema.schema.toString());
                                return Optional.empty();
                            }
                        }
                );

        return mappedData.orElse(sourceData);
    }

    private void sendPulsarMessage(Message toSend) {
        producer.newMessage()
                .key(toSend.getKey())
                .eventTime(toSend.getEventTime())
                .properties(toSend.getProperties())
                .value(toSend.getData())
                .sendAsync()
                .exceptionally(t -> {
                    log.error("Failed to send Pulsar message", t);
                    return null;
                }).thenRun(() -> {
        });

    }

    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {
                });
    }

    private byte[] parsePayload(TransitdataProperties.ProtobufSchema protobufSchema, byte[] sourceData) throws Exception {
        if (protobufSchema == TransitdataProperties.ProtobufSchema.MqttRawMessage) {
            return Mqtt.RawMessage.parseFrom(sourceData).toByteArray();
        } else if (protobufSchema == TransitdataProperties.ProtobufSchema.HfpData) {
            return Hfp.Data.parseFrom(sourceData).toByteArray();
        } else {
            throw new IllegalArgumentException("Cannot parse unknown protobuf format: " + protobufSchema.toString());
        }
    }
}
