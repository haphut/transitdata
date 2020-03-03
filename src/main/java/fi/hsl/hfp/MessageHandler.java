package fi.hsl.hfp;

import fi.hsl.common.hfp.*;
import fi.hsl.common.hfp.proto.*;
import fi.hsl.common.mqtt.proto.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;

import java.io.*;


public class MessageHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);
    private final HfpParser parser = HfpParser.newInstance();
    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    MessageHandler(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();
    }

    public void handleMessage(Message received) {
        try {
            if (TransitdataSchema.hasProtobufSchema(received, ProtobufSchema.MqttRawMessage)) {
                final long timestamp = received.getEventTime();
                byte[] data = received.getData();

                Hfp.Data converted = parseData(data, timestamp);
                sendPulsarMessage(received.getMessageId(), converted, timestamp);
            } else {
                log.warn("Received unexpected schema, ignoring.");
                ack(received.getMessageId()); //Ack so we don't receive it again
            }
        } catch (HfpParser.InvalidHfpTopicException | HfpParser.InvalidHfpPayloadException invalidHfpException) {
            log.warn("Failed to parse HFP data", invalidHfpException);
            //Ack messages with invalid data so they don't fill Pulsar backlog
            ack(received.getMessageId());
        } catch (Exception e) {
            log.error("Exception while handling message", e);
        }
    }

    private Hfp.Data parseData(byte[] data, long timestamp) throws IOException, HfpParser.InvalidHfpPayloadException, HfpParser.InvalidHfpTopicException {
        final Mqtt.RawMessage raw = Mqtt.RawMessage.parseFrom(data);
        final String rawTopic = raw.getTopic();
        final byte[] rawPayload = raw.getPayload().toByteArray();

        final HfpJson jsonPayload = parser.parseJson(rawPayload);
        Hfp.Payload payload = HfpParser.parsePayload(jsonPayload);
        Hfp.Topic topic = HfpParser.parseTopic(rawTopic, timestamp);

        Hfp.Data.Builder builder = Hfp.Data.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion())
                .setPayload(payload)
                .setTopic(topic);
        return builder.build();
    }

    private void sendPulsarMessage(MessageId received, Hfp.Data hfp, long timestamp) {
        producer.newMessage()
                //.key(dvjId) //TODO think about this
                .eventTime(timestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, ProtobufSchema.HfpData.toString())
                .value(hfp.toByteArray())
                .sendAsync()
                .whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);
                        //Should we abort?
                    } else {
                        //Does this become a bottleneck? Does pulsar send more messages before we ack the previous one?
                        //If yes we need to get rid of this
                        ack(received);
                    }
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
}
