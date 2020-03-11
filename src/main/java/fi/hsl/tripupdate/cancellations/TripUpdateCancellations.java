package fi.hsl.tripupdate.cancellations;

import com.google.protobuf.*;
import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.pulsar.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;
import org.springframework.stereotype.Service;

import javax.annotation.*;
import java.io.*;
import java.util.concurrent.*;

@Service
public class TripUpdateCancellations {

    private static final Logger log = LoggerFactory.getLogger(TripUpdateCancellations.class);

    @PostConstruct
    public void init() {
        try {
            final Config config = ConfigParser.createConfig();
            final PulsarApplication app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();
            final HSlCancellationAlertPoller poller = new HSlCancellationAlertPoller(context.getProducer(), context.getJedis(), config);

            final int pollIntervalInSeconds = config.getInt("poller.interval");
            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    poller.poll();
                } catch (InvalidProtocolBufferException e) {
                    log.error("Cancellation message format is invalid", e);
                } catch (PulsarClientException e) {
                    log.error("Pulsar connection error", e);
                    closeApplication(app, scheduler);
                } catch (IOException e) {
                    log.error("Error with HTTP connection: " + e.getMessage(), e);
                } catch (Exception e) {
                    log.error("Unknown exception at poll cycle: ", e);
                    closeApplication(app, scheduler);
                }
            }, 0, pollIntervalInSeconds, TimeUnit.SECONDS);

        } catch (Exception e) {
            log.error("Exception at Main: " + e.getMessage(), e);
        }
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        scheduler.shutdown();
        app.close();
    }
}
