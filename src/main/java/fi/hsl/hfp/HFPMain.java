package fi.hsl.hfp;

import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.pulsar.*;
import lombok.extern.slf4j.*;
import org.springframework.stereotype.*;

import javax.annotation.*;

@Component
@Slf4j
public class HFPMain {
    @PostConstruct
    public void init() {
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
