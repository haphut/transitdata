package fi.hsl;

import com.google.protobuf.*;
import com.typesafe.config.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.hfp.*;
import fi.hsl.metro.*;
import fi.hsl.rail.*;
import fi.hsl.tripupdate.arrival.*;
import fi.hsl.tripupdate.cancellations.*;
import fi.hsl.tripupdate.departure.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Rail source service sends tripupdates and alerts from trains
 * to corresponding tripupdate and alert topics.
 */
@SpringBootApplication
public class TransitdataApplication {

    private static final Logger log = LoggerFactory.getLogger(TransitdataApplication.class);

    public static void main(String[] args) {

        try {
            final Config config = fi.hsl.common.config.ConfigParser.createConfig();
            final PulsarApplication app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();
            final HslRailPoller poller = new HslRailPoller(context.getProducer(), context.getJedis(), config,
                    new RailTripUpdateService(context.getProducer()));

            final int pollIntervalInSeconds = config.getInt("poller.interval");
            final long maxTimeAfterSending = config.getDuration("poller.unhealthyAfterNotSending", TimeUnit.NANOSECONDS);
            final AtomicLong sendTime = new AtomicLong(System.nanoTime());

            if (context.getHealthServer() != null) {
                context.getHealthServer().addCheck(() -> System.nanoTime() - sendTime.get() < maxTimeAfterSending);
            }

            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    poller.poll();
                    sendTime.set(System.nanoTime());
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
            log.error("Exception at HSLRailSourceMain: " + e.getMessage(), e);
        }
        TripUpdateCancellations.main(args);
        ArrivalMain.main(args);
        DepartureMain.main(args);
        MetroMain.main(args);
        HFPMain.main(args);
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        scheduler.shutdown();
        app.close();
    }
}
