package fi.hsl.omm.alerts;

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
public class OmmAlerts {

    private static final Logger log = LoggerFactory.getLogger(OmmAlerts.class);

    public static void main(String[] args) {
    }

    @PostConstruct
    public void init() {
        try {
            final Config config = ConfigParser.createConfig();
            final String connectionString = readConnectionString();
            final int pollIntervalInSeconds = config.getInt("fi.hsl.omm.interval");
            log.info("Starting fi.hsl.omm alert source with poll interval (s): {}", pollIntervalInSeconds);

            final PulsarApplication app = PulsarApplication.newInstance(config);
            final OmmDbConnector omm = new OmmDbConnector(config, pollIntervalInSeconds, connectionString);
            final OmmAlertHandler alerter = new OmmAlertHandler(app.getContext(), omm);

            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    alerter.pollAndSend();
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
        scheduler.shutdown();
        app.close();
    }
}