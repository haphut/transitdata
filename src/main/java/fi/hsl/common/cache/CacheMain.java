package fi.hsl.common.cache;

import com.microsoft.sqlserver.jdbc.*;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.io.*;
import java.sql.*;
import java.time.*;
import java.time.temporal.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

@Service
public class CacheMain {
    private static final Logger log = LoggerFactory.getLogger(CacheMain.class);

    @Autowired
    private fi.hsl.common.pulsar.PulsarApplicationContext context;
    @Value("${pulsar.cache.connectionString}")
    private String connectionString;

    private ScheduledExecutorService executor;
    private AtomicBoolean processingActive = new AtomicBoolean(false);

    private RedisUtils redisUtils;
    private QueryUtils queryUtils;
    @Value("${bootstrapper.queryHistoryInDays}")
    private int queryHistoryInDays;
    @Value("${bootstrapper.queryFutureInDays}")
    private int queryFutureInDays;

    private static long secondsUntilNextEvenHour() {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime nextHour = now.plusHours(1);
        OffsetDateTime evenHour = nextHour.truncatedTo(ChronoUnit.HOURS);
        log.debug("Current time is " + now.toString() + ", next even hour is at " + evenHour.toString());
        return Duration.between(now, evenHour).getSeconds();
    }


    @PostConstruct
    public void init() throws Exception {
        String connectionString = "";

        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = fi.hsl.common.config.ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string");
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read the DB connection string from the file", e);
        }

        if (connectionString.equals("")) {
            log.error("Connection string empty, aborting.");
            System.exit(1);
        }
        this.start();
    }

    public void start() throws Exception {
        initialize();
        startPolling();
        //Invoke manually the first task immediately
        process();

        // Block main thread in order to keep PulsarApplication alive
        // TODO: Refactor
        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private void initialize() {
        redisUtils = new RedisUtils(context);
        log.info("Fetching data from -" + queryHistoryInDays + " days to +" + queryFutureInDays + " days");
        queryUtils = new QueryUtils(queryHistoryInDays, queryFutureInDays);
    }

    private void startPolling() {
        final long periodInSecs = 60 * 60;
        final long delayInSecs = secondsUntilNextEvenHour();

        log.info("Starting scheduled poll task. First poll execution in " + delayInSecs + "secs");
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                log.info("Poll timer tick");
                queryUtils.updateFromToDates();
                process();
            }
        };

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(task, delayInSecs, periodInSecs, TimeUnit.SECONDS);
    }

    private void process() {
        if (!processingActive.getAndSet(true)) {
            log.info("Fetching data");
            try (Connection connection = DriverManager.getConnection(connectionString)) {
                final QueryProcessor queryProcessor = new QueryProcessor(connection);
                final JourneyResultSetProcessor journeyResultSetProcessor = new JourneyResultSetProcessor(redisUtils, queryUtils);
                final StopResultSetProcessor stopResultSetProcessor = new StopResultSetProcessor(redisUtils, queryUtils);
                final MetroJourneyResultSetProcessor metroJourneyResultSetProcessor = new MetroJourneyResultSetProcessor(redisUtils, queryUtils);

                queryProcessor.executeAndProcessQuery(journeyResultSetProcessor);
                queryProcessor.executeAndProcessQuery(stopResultSetProcessor);
                queryProcessor.executeAndProcessQuery(metroJourneyResultSetProcessor);

                redisUtils.updateTimestamp();

                log.info("All data processed, thank you.");
            } catch (SQLServerException sqlServerException) {
                String msg = "SQLServerException during query, Driver Error code: "
                        + sqlServerException.getErrorCode()
                        + " and SQL State: " + sqlServerException.getSQLState();
                log.error(msg, sqlServerException);
                shutdown();
            } catch (Exception e) {
                log.error("Unknown exception during query ", e);
                shutdown();
            } finally {
                processingActive.set(false);
            }
        } else {
            log.warn("Processing already active, will not launch another task.");
        }
    }

    private void shutdown() {
        log.warn("Shutting down the application.");
        if (redisUtils.jedis != null) {
            redisUtils.jedis.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        log.info("Shutdown completed, bye.");
    }

}
