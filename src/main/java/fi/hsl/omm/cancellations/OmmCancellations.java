package fi.hsl.omm.cancellations;

import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.pulsar.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

@Service
public class OmmCancellations {

    private static final Logger log = LoggerFactory.getLogger(OmmCancellations.class);

    @PostConstruct
    public void init() {
        PulsarApplication appRef = null;
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            final Config config = ConfigParser.createConfig();

            String cancellationsFromTime = config.getString("omm.cancellationsFromTime");
            // omm.cancellationsFromTime should have value "NOW" or "PAST" (NOW for transitdata, PAST for transitlog)
            CancellationSourceType sourceType = CancellationSourceType.fromString(cancellationsFromTime);

            if (sourceType == CancellationSourceType.FROM_PAST) {
                log.info("Creating OMM cancellation source for past, ongoing and future cancellations");
            } else if (sourceType == CancellationSourceType.FROM_NOW) {
                log.info("Creating OMM cancellation source for ongoing and future cancellations");
            } else {
                log.error("Failed to get source type from CANCELLATIONS_FROM_TIME -env variable, exiting application");
                log.info("CANCELLATIONS_FROM_TIME -env variable should be either 'NOW' (for transitdata) or 'PAST' (for transitlog)");
                System.exit(1);
            }

            final String connectionString = readConnectionString();
            final PulsarApplication app = PulsarApplication.newInstance(config);
            appRef = app;
            final PulsarApplicationContext context = app.getContext();
            final OmmConnector omm = OmmConnector.newInstance(context, connectionString, sourceType);
            final int pollIntervalInSeconds = config.getInt("omm.interval");

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    omm.queryAndProcessResults(pollIntervalInSeconds);
                } catch (PulsarClientException e) {
                    log.error("Pulsar connection error", e);
                    closeApplication(app, scheduler);
                } catch (SQLException e) {
                    log.error("SQL exception", e);
                    closeApplication(app, scheduler);
                } catch (Exception e) {
                    log.error("Unknown exception at poll cycle: ", e);
                    closeApplication(app, scheduler);
                }
            }, 0, pollIntervalInSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception at Main: " + e.getMessage(), e);
            closeApplication(appRef, scheduler);
        }
    }

    private static String readConnectionString() throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING")
                    .orElse("/run/secrets/db_conn_string");
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read DB connection string from secrets", e);
            throw e;
        }

        if (connectionString.isEmpty()) {
            throw new Exception("Failed to find DB connection string, exiting application");
        }
        return connectionString;
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
        }
        if (app != null) {
            app.close();
        }
    }
}
