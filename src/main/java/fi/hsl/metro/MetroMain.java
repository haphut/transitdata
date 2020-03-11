package fi.hsl.metro;

import com.typesafe.config.*;
import fi.hsl.common.config.*;
import fi.hsl.common.pulsar.*;
import lombok.extern.slf4j.*;
import org.springframework.stereotype.*;

import javax.annotation.*;

@Service
@Slf4j
public class MetroMain {

    @PostConstruct
    public void init() {
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