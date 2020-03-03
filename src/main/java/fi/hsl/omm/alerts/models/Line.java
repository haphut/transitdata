package fi.hsl.omm.alerts.models;


import lombok.*;

import java.util.*;

@Data
public class Line {
    public List<Route> routes;
    private long gid;

    public Line() {
    }

    public Line(long gid) {
        this.gid = gid;
        this.routes = new ArrayList<>();
    }

    public void addRouteToLine(Route route) {
        routes.add(route);
    }
}
