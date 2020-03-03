package fi.hsl.omm.alerts;

import fi.hsl.omm.alerts.models.*;

import java.sql.*;
import java.util.*;

public interface StopPointDAO {
    Map<Long, List<StopPoint>> getAllStopPoints() throws SQLException;
}
