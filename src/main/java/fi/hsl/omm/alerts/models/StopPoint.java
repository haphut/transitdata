package fi.hsl.omm.alerts.models;

import fi.hsl.omm.alerts.*;
import lombok.*;

import java.time.*;
import java.util.*;

@Data
public class StopPoint {
    private long gid;
    private String stopId;
    private Optional<LocalDateTime> existsFromDate;
    private Optional<LocalDateTime> existsUptoDate;

    public StopPoint() {
    }

    public StopPoint(long gid, String id) {
        this.gid = gid;
        this.stopId = id;
        this.existsFromDate = Optional.empty();
        this.existsUptoDate = Optional.empty();
    }

    public StopPoint(long gid, String id, String existsFromDate, String existsUptoDate) {
        this.gid = gid;
        this.stopId = id;
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
