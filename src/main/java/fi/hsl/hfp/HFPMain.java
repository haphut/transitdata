package fi.hsl.hfp;

import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.pulsar.*;
import org.slf4j.*;

public class HFPMain {
    private static final Logger log = LoggerFactory.getLogger(HFPMain.class);

    public static void main(String[] args) {
        log.info("Starting Hfp Parser");
        Config config = ConfigParser.createConfig();
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            PulsarApplicationContext context = app.getContext();
            MessageHandler router = new MessageHandler(context);
            log.info("Start handling the messages");
            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }
}
