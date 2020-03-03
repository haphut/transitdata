package fi.hsl.metro;

import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.pulsar.*;
import org.slf4j.*;

public class MetroMain {
    private static final Logger log = LoggerFactory.getLogger(MetroMain.class);

    public static void main(String[] args) {
        log.info("Starting Metro-ats Parser");
        Config config = ConfigParser.createConfig();
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();

            MetroEstimatesFactory metroEstimatesFactory = new MetroEstimatesFactory(context);
            MessageHandler router = new MessageHandler(context, metroEstimatesFactory);

            log.info("Start handling the messages");
            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }
}