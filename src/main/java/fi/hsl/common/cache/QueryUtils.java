package fi.hsl.common.cache;

import lombok.extern.slf4j.*;

import java.time.*;
import java.time.format.*;
import java.time.temporal.*;

@Slf4j
public class QueryUtils {

    final String DVJ_ID = "dvj_id";
    final String DIRECTION = "direction";
    final String ROUTE_NAME = "route";
    final String START_TIME = "start_time";
    final String OPERATING_DAY = "operating_day";
    final String STOP_NUMBER = "stop_number";
    public String from;
    public String to;
    private int queryHistoryInDays;
    private int queryFutureInDays;

    QueryUtils(int queryHistoryInDays, int queryFutureInDays) {
        this.queryHistoryInDays = queryHistoryInDays;
        this.queryFutureInDays = queryFutureInDays;
        this.updateFromToDates();
    }

    void updateFromToDates() {
        this.from = formatDate(-queryHistoryInDays);
        this.to = formatDate(queryFutureInDays);
        log.info("Fetching data from {} to {}", this.from, this.to);
    }

    private static String formatDate(int offsetInDays) {
        LocalDate now = LocalDate.now();
        LocalDate then = now.plus(offsetInDays, ChronoUnit.DAYS);
        String formattedString = DateTimeFormatter.ISO_LOCAL_DATE.format(then);
        log.debug("offsetInDays results to date " + formattedString);
        return formattedString;
    }
}
