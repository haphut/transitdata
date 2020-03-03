package fi.hsl.metro.models;


import fi.hsl.metro.*;
import lombok.*;

import java.util.*;

@Data
public class MetroEstimate {
    public String routeName;
    public MetroTrainType trainType;
    public MetroProgress journeySectionprogress;
    public List<MetroStopEstimate> routeRows;
    private String beginTime;
    private String endTime;

    public void setBeginTime(String beginTime) {
        Optional<String> maybeBeginTime = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(beginTime);
        this.beginTime = maybeBeginTime.orElse(null);
    }

    public void setEndTime(String endTime) {
        Optional<String> maybeEndTime = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(endTime);
        this.endTime = maybeEndTime.orElse(null);
    }
}
