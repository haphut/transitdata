package fi.hsl.omm.alerts;

import fi.hsl.omm.alerts.models.*;

import java.sql.*;
import java.util.*;

public interface LineDAO {
    Map<Long, Line> getAllLines() throws SQLException;
}
