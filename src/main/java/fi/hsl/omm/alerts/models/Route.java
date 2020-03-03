package fi.hsl.omm.alerts.models;

import fi.hsl.omm.alerts.*;
import lombok.*;
import org.slf4j.*;

import java.time.*;
import java.util.*;

@Data
public class Route {
    static final Logger log = LoggerFactory.getLogger(Route.class);
    private long lineGid;
    private String routeId;
    private Optional<LocalDateTime> existsFromDate;
    private Optional<LocalDateTime> existsUptoDate;

    public Route() {
    }

    public Route(long lineGid, String id) {
        this.lineGid = lineGid;
        this.routeId = id;
        this.existsFromDate = Optional.empty();
        this.existsUptoDate = Optional.empty();
    }

    public Route(long lineGid, String id, String existsFromDate, String existsUptoDate) {
        this.lineGid = lineGid;
        this.routeId = id;
        this.existsFromDate = getDateOrEmpty(existsFromDate);
        this.existsUptoDate = getDateOrEmpty(existsUptoDate);
    }

    private Optional<LocalDateTime> getDateOrEmpty(String dateStr) {
        try {
            return Optional.of(DAOImplBase.parseOmmLocalDateTime(dateStr));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
