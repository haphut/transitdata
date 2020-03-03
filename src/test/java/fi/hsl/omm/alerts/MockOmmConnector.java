package fi.hsl.omm.alerts;


import fi.hsl.omm.alerts.models.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public class MockOmmConnector {

    private BulletinDAO bulletinDAO;
    private LineDAO lineDAO;
    private StopPointDAO stopPointDAO;

    private String filename;

    private MockOmmConnector(String filename) {
        this.filename = filename;
    }

    public static MockOmmConnector newInstance(String filename) throws Exception {
        MockOmmConnector connector = new MockOmmConnector(filename);
        connector.initialize();
        return connector;
    }

    void initialize() throws Exception {
        List<String> content = readFileContentFromResources(filename);

        List<Bulletin> bulletins = parseBulletinsFromTsvContent(content);
        bulletinDAO = new BulletinDAOMock(bulletins);

        lineDAO = new LineDAOMock(bulletins);
        stopPointDAO = new StopPointDAOMock(bulletins);
    }

    private List<String> readFileContentFromResources(String filename) throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource(filename);

        boolean first = true;
        List<String> out = new LinkedList<>();
        try (InputStream in = url.openStream()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                if (first) {
                    //Ignore title lines
                    first = false;
                    continue;
                }
                out.add(line);
            }
        }
        return out;
    }

    private List<Bulletin> parseBulletinsFromTsvContent(List<String> lines) throws Exception {
        //We've used tabs as separators, because content includes commas.
        return lines.stream().map(BulletinDAOMock::parseBulletinFromTsv).collect(Collectors.toList());
    }

    public static String lineGidToRouteId(long gid) {
        return "line-" + Long.toString(gid);
    }

    public static String stopGidtoStopPointId(long gid) {
        return "stop-" + Long.toString(gid);
    }

    public BulletinDAO getBulletinDAO() {
        return bulletinDAO;
    }

    public LineDAO getLineDAO() {
        return lineDAO;
    }

    public StopPointDAO getStopPointDAO() {
        return stopPointDAO;
    }

    static class LineDAOMock implements LineDAO {
        Map<Long, Line> lines;

        LineDAOMock(List<Bulletin> bulletins) {
            lines = bulletins.stream()
                    .flatMap(bulletin -> bulletin.getAffectedLineGids().stream())
                    .collect(Collectors.toMap(
                            gid -> gid,
                            gid -> {
                                Line line = new Line(gid);
                                line.addRouteToLine(new Route(gid, lineGidToRouteId(gid), "2019-01-18 13:01:35.270", "2019-12-18 13:01:35.270"));
                                return line;
                            },
                            (oldId, newId) -> oldId) //Merge by just throwing away duplicates
                    );
        }

        @Override
        public Map<Long, Line> getAllLines() throws SQLException {
            return lines;
        }
    }

    static class StopPointDAOMock implements StopPointDAO {
        Map<Long, List<StopPoint>> stops = new HashMap<>();

        StopPointDAOMock(List<Bulletin> bulletins) {
            for (Bulletin bulletin : bulletins) {
                for (long stopGid : bulletin.getAffectedStopGids()) {
                    if (!stops.containsKey(stopGid)) {
                        List stopsList = new ArrayList<StopPoint>();
                        stops.put(stopGid, stopsList);
                    }
                    stops.get(stopGid).add(new StopPoint(stopGid, stopGidtoStopPointId(stopGid), "2019-01-18 13:01:35.270", "2019-06-18 13:01:35.270"));
                }
            }
        }

        @Override
        public Map<Long, List<StopPoint>> getAllStopPoints() throws SQLException {
            return stops;
        }
    }

}
