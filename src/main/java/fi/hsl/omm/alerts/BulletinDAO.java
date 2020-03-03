package fi.hsl.omm.alerts;

import fi.hsl.omm.alerts.models.*;

import java.sql.*;
import java.util.*;

public interface BulletinDAO {
    List<Bulletin> getActiveBulletins() throws SQLException;
}
