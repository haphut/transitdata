package fi.hsl.rail;

import com.google.transit.realtime.*;
import com.typesafe.config.*;
import lombok.extern.slf4j.*;
import org.apache.pulsar.client.api.*;
import redis.clients.jedis.*;

import java.io.*;
import java.net.*;

@Slf4j
public
class HslRailPoller {
    private final Producer<byte[]> producer;
    private final Jedis jedis;
    private final String railUrlString;
    private final RailTripUpdateService railTripUpdateService;

    public HslRailPoller(Producer<byte[]> producer, Jedis jedis, Config config, RailTripUpdateService railTripUpdateService) {
        this.railUrlString = config.getString("poller.railurl");
        this.producer = producer;
        this.jedis = jedis;
        this.railTripUpdateService = railTripUpdateService;
    }

    public void poll() throws IOException {
        GtfsRealtime.FeedMessage feedMessage = readFeedMessage(railUrlString);
        handleFeedMessage(feedMessage);
    }

    static com.google.transit.realtime.GtfsRealtime.FeedMessage readFeedMessage(String url) throws IOException {
        return readFeedMessage(new URL(url));
    }

    private void handleFeedMessage(com.google.transit.realtime.GtfsRealtime.FeedMessage feedMessage) throws PulsarClientException {
        railTripUpdateService.sendRailTripUpdates(feedMessage);
    }

    static com.google.transit.realtime.GtfsRealtime.FeedMessage readFeedMessage(URL url) throws IOException {
        log.info("Reading fi.hsl.rail feed messages from " + url);

        try (InputStream inputStream = url.openStream()) {
            return com.google.transit.realtime.GtfsRealtime.FeedMessage.parseFrom(inputStream);
        }
    }

}
