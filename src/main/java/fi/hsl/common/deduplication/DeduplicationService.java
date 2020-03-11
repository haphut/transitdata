package fi.hsl.common.deduplication;

import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.pulsar.*;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;

@Service
public class DeduplicationService {
    private static final Logger log = LoggerFactory.getLogger(DeduplicationService.class);

    @Autowired
    private PulsarApplicationContext pulsarApplicationContext;

    @PostConstruct
    public void init() {
        log.info("Starting Hfp De-duplicator");
        Config config = ConfigParser.createConfig();
        Analytics analytics = null;
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();
            analytics = new Analytics(context.getConfig());

            Deduplicator router = new Deduplicator(context, analytics);

            log.info("Start handling the messages");
            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
            if (analytics != null)
                analytics.close();
        }
    }
}
